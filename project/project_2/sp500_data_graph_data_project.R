# load packages
if(!require(pacman)){install.packages("pacman"); require(pacman)}
pacman::p_load(tictoc)

# set environment
readRenviron("~/I/config/.env")

# set local environment variables using global environment
PACKAGE_PATH <- Sys.getenv("PACKAGE_PATH")
PROJECT_HOME_DIRECTORY <- Sys.getenv("PROJECT_HOME_DIRECTORY")
FUNCTION_DIRECTORY <- Sys.getenv("FUNCTION_DIRECTORY")

# set project home directory
setwd(PROJECT_HOME_DIRECTORY)

# load packages
packages <- read.csv(PACKAGE_PATH, header = FALSE)
pacman::p_load(char = as.vector(packages$V1))

### GET SYMBOLS ###

# load a list of tracking entity
sp500 <- readit::readit(file.choose()) 
symbols <- sp500 %>%
        dplyr::arrange(symbol) %>%
        dplyr::select(symbol) %>%
        .$symbol

# start timer
tic("start timer")

# detect, use multicores
numCores <- parallel::detectCores()

# create a simple cluster on the local machine using all available threads
cl <- parallel::makeCluster(detectCores(), methods = FALSE)

# register our cluster
doParallel::registerDoParallel(cl)

# check data availability
symbolsCheck <- foreach::foreach(i = 1:length(symbols), .errorhandling = 'remove') %dopar% { quantmod::getSymbols(symbols[i]) } %>% unlist  # change .errorhandling = 'pass' to see error

# print out a list of invalid tickers
if(length(setdiff(symbols, symbolsCheck)) >0){
        
        errorSymbols = setdiff(symbols, symbolsCheck)
        
        sapply(1:length(errorSymbols), function(x){
                print(paste0("the symbol ", errorSymbols[x], " cannot be fetched from quantmod"))
        })
        
        cat("###########################################\n###########################################\n")
        print(paste0("there are ", 
                     length(errorSymbols), 
                     " symbols that cannot be fetched from quantmod"))
        cat("###########################################\n###########################################\n")
        
}

# nested for() loop, return a single list of xts objects from the valid symbols
symbols <- symbolsCheck

symbolsDf <- data.frame(symbol = symbols) %>%
        dplyr::arrange(symbol) %>%
        dplyr::mutate(symbol = as.character(symbol),
                      partition = 1:nrow(.), 
                      partition = cut(partition, 
                                      breaks = 5, 
                                      labels = wrapr::qc(L1, L2, L3, L4, L5)) %>%
                              as.character) 

# setup
part = symbolsDf$partition %>% unique
partLength = length(part)
xtsList = vector(mode = "list")
from.dat <- as.Date("08/31/20", format = "%m/%d/%y")
to.dat <- as.Date("10/01/20", format = "%m/%d/%y")

# start nested for() loop
for(i in 1:partLength){
        s = symbolsDf %>%
                dplyr::filter(partition == part[i]) %>%
                dplyr::select(symbol) %>%
                .$symbol
        
        xtsListTemp <- try({
                  foreach::foreach(j = 1:length(s)) %dopar% {quantmod::getSymbols(s[j], 
                                                                                  src = 'yahoo',
                                                                                  from = from.dat,
                                                                                  to = to.dat,
                                                                                  env = NULL, 
                                                                                  adjusted = TRUE)}
                }, silent = TRUE)
                
        xtsList <- c(xtsList, xtsListTemp)
        
        Sys.sleep(10)
}

# get names for xtsList
names(xtsList) <- symbols

# drop retrieval errors from list
if(any(sapply(xtsList, class) %in% c("try-error", "character"))){
        
        i = which(sapply(xtsList, class) %in% c("try-error", "character"))
        
        errorSymbols = names(xtsList)[i]
        
        xtsList[i] <- NULL
        
        sapply(1:length(errorSymbols), function(x){
                print(paste0("the symbol ", errorSymbols[x], " cannot be fetched from quantmod"))
        })
        
        cat("###########################################\n###########################################\n")
        print(paste0("there are ", 
                     length(errorSymbols), 
                     " symbols that cannot be fetched from quantmod"))
        cat("###########################################\n###########################################\n")
        
}

# print result of xtsList
print(paste0("there are ", 
             length(xtsList), 
             " stocks retrieved for the period between ", 
             from.dat, 
             " and ", 
             to.dat))

# stop the cluster
parallel::stopCluster(cl)

# source all functions
sapply(paste(FUNCTION_DIRECTORY, grep(pattern = "\\.[Rr]$", list.files(FUNCTION_DIRECTORY), value = TRUE), sep = "/"), function(x) source(x)) %>% invisible()

# stop - set up
toc(log = TRUE)

######################################################################
# create df for graphing

# transformation() - a custom function for calculating Up, Down, Same for day-over-day change
transformation <- function(xts, lday = 1, percent_change = 0.03){
        
        # symbol
        symbol <- stringr::str_extract_all(names(xts), pattern = "^[[:alpha:]].*\\.") %>% unlist %>% unique %>% gsub("\\.", "", .)
        
        # rename columns
        names(xts) <- gsub("^[[:alpha:]].*\\.", "", names(xts)) %>% stringr::str_to_lower(.)
        
        # turn it into data.frame
        xts <- as.data.frame(xts)
        
        # insert columns
        xts <- xts %>%
                dplyr::mutate(
                        # symbol
                        symbol = symbol,
                        # date
                        date = row.names(xts) %>% lubridate::ymd(.),
                        year = lubridate::year(date),
                        month = lubridate::month(date),
                        day = lubridate::day(date),
                        # Lag
                        cl_l = quantmod::Lag(xts[, "close"], lday) %>% as.vector,
                        # Lag "diff"
                        cl_l_diff = close - cl_l,
                        # direction
                        direction = dplyr::case_when( (cl_l_diff / cl_l) > percent_change ~ 'Up',
                                                (cl_l_diff / cl_l) < -1 * percent_change ~ 'Down',
                                                TRUE ~ 'Same' )
                ) %>%
                dplyr::select(symbol, date, year, month, day,
                              open, high, low, close, volume, adjusted,
                              everything()) %>%
                dplyr::arrange(date)
        
        # return xts
        return(xts)
        
}

xtsDfList <- vector(mode = "list", length = length(xtsList))

for(i in 1:length(xtsDfList)){
        
        xtsObj = xtsList[[i]]
        xtsDfList[[i]] = transformation(xtsObj, lday = 1)
        
}

xtsDf <- xtsDfList %>%
        dplyr::bind_rows() %>%
        dplyr::filter(month == 9) %>%
        dplyr::mutate(datekey = paste0("d", day)) %>%
        dplyr::select(symbol, datekey, close, previous_close = cl_l, direction) %>%
        dplyr::inner_join(., sp500, by = 'symbol')

write.csv(xtsDf, "clipboard-16384", row.names = FALSE)



