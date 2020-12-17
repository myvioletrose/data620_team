############################################################ SET UP ############################################################

# start time
start_time = Sys.time()

# load packages
if(!require(pacman)){install.packages("pacman"); require(pacman)}
pacman::p_load(tictoc)

# set environment
readRenviron("~/I/config/.env")

# set local environment variables using global environment
PACKAGE_PATH <- Sys.getenv("PACKAGE_PATH")
PROJECT_HOME_DIRECTORY <- Sys.getenv("PROJECT_HOME_DIRECTORY")
SP500_LIST_PATH <- Sys.getenv("SP500_LIST_PATH")
FUNCTION_DIRECTORY <- Sys.getenv("FUNCTION_DIRECTORY")
ALPHA_VANTAGE_API <- Sys.getenv("ALPHA_VANTAGE_API")
PLOT_PATH <- Sys.getenv("PLOT_PATH")
VOTES_PATH = Sys.getenv("VOTES_PATH")
TA_FOLDER <- Sys.getenv("TA_FOLDER")
DIAGNOSTIC_FOLDER <- Sys.getenv("DIAGNOSTIC_FOLDER")
DATA_DIRECTORY <- Sys.getenv("DATA_DIRECTORY")
TWITTER_API_KEY <- Sys.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET <- Sys.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN <- Sys.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET <- Sys.getenv("TWITTER_ACCESS_TOKEN_SECRET")

# set project home directory
setwd(PROJECT_HOME_DIRECTORY)

# load packages
packages <- read.csv(PACKAGE_PATH, header = FALSE)
pacman::p_load(char = as.vector(packages$V1))

############################################################ GET SYMBOLS ############################################################

# load a list of tracking entity
sp500 <- readit::readit(SP500_LIST_PATH) 
symbols <- sp500 %>%
        dplyr::arrange(symbol) %>%
        dplyr::select(symbol) %>%
        .$symbol

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
from.dat <- as.Date("01/01/19", format = "%m/%d/%y")
#to.dat <- as.Date("11/30/20", format = "%m/%d/%y")
to.dat <- Sys.Date()


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

# source all functions
sapply(paste(FUNCTION_DIRECTORY, grep(pattern = "\\.[Rr]$", list.files(FUNCTION_DIRECTORY), value = TRUE), sep = "/"), function(x) source(x)) %>% invisible()

# stop the cluster
parallel::stopCluster(cl)

# end time
end_time = Sys.time()

cat("#########################################################################################################")
cat(glue::glue(paste0("the start time is ", start_time, "\n",
           "the end time is ", end_time, "\n",
           "the total run time for extracting {length(xtsList)} symbols is approximately ", round(difftime(end_time, 
                                                                                                           start_time, 
                                                                                                           units = "mins"), 
                                                                                                  2), " mins")))

############################################################ DATA PREPROCESSING ############################################################

# subset period
start_date = '2020-12-01'
end_date = '2020-12-11'

# make a copy of the xtsList
XTSLIST = xtsList

# get November data only
xList <- lapply(XTSLIST, function(x) x[paste0(gsub("-", "", start_date), "/", gsub("-", "", end_date))])

# transform - part I: extract "close" price, gather data
xDf <- lapply(1:length(xList), function(x) xList[[x]] %>% 
                      as.data.frame %>%
                      tibble::rownames_to_column() %>%
                      dplyr::mutate(symbol = names(xList[x]),
                                    index = 1:nrow(.)) %>%
                      dplyr::select(date = rowname,
                                    index,
                                    symbol, 
                                    close = contains("Close"))) %>%
        dplyr::bind_rows() %>%
        dplyr::inner_join(., sp500, by = "symbol")

dim(xDf); head(xDf, 50)

# transform - part II: calculate begin, end prices, percent_change, rank
xDf2 <- xDf %>%
        dplyr::filter(index == 1 | index == max(xDf$index)) %>%
        dplyr::select(-index) %>%
        tidyr::spread(., "date", "close") 

# remove NA
xDf2 <- xDf2[complete.cases(xDf2), ]

# rename columns
names(xDf2) <- c("symbol", "company_name", "sector", "begin", "end")

# calculate percent_change, rank
xDf2 <- xDf2 %>%
        dplyr::mutate(percent_change = (end - begin) / begin) %>%
        arrange(desc(percent_change)) %>%
        dplyr::mutate(rank = 1:nrow(.)) %>%
        dplyr::select(symbol, company_name, sector, everything())

# top, bottom 5 performing stocks
head(xDf2, 5); tail(xDf2, 5)

# calculate percent_change, rank by sector
xDf3 <- xDf2 %>%
        group_by(sector) %>%
        summarise(avg_percent_change = mean(percent_change)) %>%
        arrange(desc(avg_percent_change))

xDf3

############################################################ twitteR ############################################################

## set up oauth
setup_twitter_oauth(TWITTER_API_KEY, 
                    TWITTER_API_SECRET,
                    TWITTER_ACCESS_TOKEN,
                    TWITTER_ACCESS_TOKEN_SECRET)

max_num_tweets = 500

lookUpDf <- data.frame(tickers = xDf$symbol %>% unique %>% sort) %>%
        dplyr::mutate(tickers = as.character(tickers),
                      partition = cut(1:nrow(.), breaks = nrow(.)/ 50, labels = paste0("partiton_", 1:10)) %>% as.character) 

num_of_partition = length(unique(lookUpDf$partition))

list_of_list <- vector(mode = "list", length = num_of_partition)
names(list_of_list) = unique(lookUpDf$partition)

tic()

for(i in 1:num_of_partition){
        
        par = unique(lookUpDf$partition)[i]
        
        p = lookUpDf %>% dplyr::filter(partition == par) %>%
                dplyr::select(tickers) %>%
                .$ticker
        
        pDf <- vector(mode = "list", length = length(p))
        names(pDf) <- p
        
        for(j in 1:length(p)){
                
                # ticker
                s = p[j]
                
                # search string
                ss = paste0("$", s, " + news")         
                #ss = paste0(s, " + stock")         
                
                # search twitter
                t = twitteR::searchTwitter(searchString = ss,
                                           lang = "en",
                                           n = max_num_tweets,
                                           #resultType = "popular",
                                           #since = as.character(Sys.Date()-9),
                                           #until = as.character(Sys.Date()-8)
                                           since = as.character(Sys.Date()-7)
                ) %>%
                        twitteR::twListToDF() %>%
                        dplyr::mutate(symbol = s)
                
                # store each result into a list
                pDf[[j]] <- t
                
        }
        
        # write output to each partition
        list_of_list[[i]] <- pDf %>% dplyr::bind_rows()
        
        #Sys.sleep(300)
        
}

toc()

# combine list into one df
tDf = list_of_list %>% dplyr::bind_rows() %>% distinct() %>%
        dplyr::inner_join(., sp500, by = "symbol")

# tDf1 - isRetweet == FALSE
tDf1 <- tDf %>%
        dplyr::filter(isRetweet == FALSE) %>%
        dplyr::select(sector, symbol, company_name, id, text, favoriteCount, retweetCount, created, screenName) %>% 
        distinct() %>%
        arrange(sector, symbol, created, screenName, id)

dim(tDf1)

data.frame(col = names(tDf1)) %>%
        dplyr::mutate(class = sapply(tDf1, class),
                      missing = colSums(is.na(tDf1)),
                      unique = sapply(tDf1, function(x) length(unique(x))))

head(tDf1) 

############################################################ export ############################################################
# export xDf2
write.csv(xDf2, "stock_data_month_to_date_20201211.csv", row.names = FALSE)

# export tDF1
write.csv(tDf1, file = "tweets_Df.csv", row.names = FALSE)

# ggplot - export index chart
visObj <- xDf %>%
        dplyr::select(date, symbol, sector, price = close) %>%
        dplyr::mutate(date = as.Date(date)) %>%
        dplyr::inner_join(., xDf2 %>%
                                  dplyr::select(symbol, price_initial = begin),
                          by = "symbol") %>%
        dplyr::mutate(price = round(price / price_initial, 2)-1) %>%
        dplyr::select(-price_initial)

sectors = visObj$sector %>% unique

last_date_in_xDf = max(xDf$date)

# save chart by sector
for(i in 1:length(sectors)){
        
        g = visObj %>%
                dplyr::filter(symbol != "NBL") %>%
                dplyr::filter(sector == sectors[i]) %>%
                ggplot(., aes(date, price)) +
                geom_line(aes(col = price)) +
                geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
                geom_hline(yintercept = 0.5, linetype = "dashed", color = "red") +
                #theme_bw() +
                theme_classic() +
                #theme_blackboard() +
                #theme_minimal() +
                bdscale::scale_x_bd(business.dates = visObj %>% 
                                            lplyr::pull(date) %>%
                                            unique,
                                    max.major.breaks = length(visObj$date %>% unique()),
                                    labels = scales::date_format("%Y-%m-%d")) +
                theme(legend.position = "none", 
                      plot.title = element_text(hjust = 0.5),
                      axis.text.x = element_text(hjust = 1, angle = 60),
                      axis.title.x = element_blank(),
                      axis.title.y = element_blank(),
                      legend.title = element_blank(),
                      strip.background = element_blank()) +
                facet_wrap(~symbol, ncol = 7) +
                ggtitle(sectors[i])
        
        ggsave(filename = paste0(sectors[i], "_", last_date_in_xDf, ".png"), 
               plot = g, 
               #units = "mm", width = 750, height = 400, dpi = "print"
               #units = "mm", width = 650, height = 350, dpi = "print"
               units = "mm", width = 500, height = 300, dpi = 500
        )
        
}









