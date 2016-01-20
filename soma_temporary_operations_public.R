
### Collect Public SOMA Temporary Operations Data

soma_temporary_operations_public <- function(task = c("update","create")){

  require("DBI")
  require("RPostgreSQL")
  require("XML")
  require("gdata")
  require("tis")

  task <- match.arg(task)
  
  # parameters to change if run elsewhere
  setwd("/mma/prod/MBS/MBS_Portfolio_Analytics/")
  dbname <- "ma"
  host <- "sqldev"  
  schema_location <- "set search_path to mma;"
  all_permissions <- c("mma")
  select_permissions <- c("mma","dma")
  
  # connect to SQL
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv,dbname=dbname,host=host)
  searchPath <- dbSendQuery(con,statement=schema_location)

  if(task == "create"){
    # download and bring in the data
    date1 <- format(previousBusinessDay(as.Date("2007-01-03")),"%m%d%Y")
    date2 <- format(previousBusinessDay(today()),"%m%d%Y")
  }
  if(task == "update"){
    lastdate <- dbGetQuery(con, "select max(deal_date) from soma_temporary_operations;")$max
    date1 <- format(nextBusinessDay(lastdate),"%m%d%Y")
    date2 <- format(today(),"%m%d%Y")
  }
  # URL has changed in the past
  url <-
    paste0("https://websvcgatewayx2.frbny.org/autorates_tomo_external/services/v1_0/tomo/retrieveHistoricalExcel?f=", date1,"&t=",date2,"&ctt=true&&cta=true&ctm=true")

  download.file(url, "tomo.xls","wget")

  tomo <- read.xls("tomo.xls", stringsAsFactors = FALSE)

  # clean the data
  colnames(tomo) <- tolower(colnames(tomo))
  colnames(tomo) <- gsub("\\.","_",colnames(tomo))
  tomo[tomo == "N/A"] <- NA

  # format dates
  dates <- c("deal_date", "delivery_date", "maturity_date")
  for(d in dates){
    tomo[,d] <-
      format(as.Date(tomo[,d],format="%m/%d/%Y"),"%Y-%m-%d")
  }

  # format numbers
  tomo <- tomo[order(tomo$deal_date),]
  nums <- c("tsy_submit", "tsy_accept", "tsy_stop_out", "tsy_award",
            "tsy_wght_avg", "tsy_high", "tsy_low", "tsy_pctatstopout", "agy_submit", "agy_accept", "agy_stop_out", "agy_award", "agy_wght_avg", "agy_high", "agy_low", "agy_pctatstopout", "mbs_submit", "mbs_accept", "mbs_stop_out", "mbs_award", "mbs_wght_avg", "mbs_high", "mbs_low", "mbs_pctatstopout", "total_submit","total_accept")
  for(n in nums){
    tomo[,n] <- as.numeric(tomo[,n])
  }

  # push to sql
  if(task == "create"){
    
    if(dbExistsTable(con, "soma_temporary_operations")){
      dbSendQuery(con, "drop table soma_temporary_operations;")
    }
    dbWriteTable(con, "soma_temporary_operations", tomo, row.names = FALSE)
    dbSendQuery(con,statement = "set client_min_messages to warning;")
    dbSendQuery(con, statement = "alter table soma_temporary_operations add primary key (op_id);")
  
    for(n in nums){
      dbSendQuery(con, statement =
                    paste("alter table soma_temporary_operations alter column ",n," type numeric using ",n,"::numeric",sep=""))
    }

  # handle permissions

  # all
    for(group in all_permissions){
      command <- paste0("grant all on soma_temporary_operations to ",group,";")
      dbSendQuery(con, statement = command)
    }
  # select
    for(group in select_permissions){
      command <- paste0("grant select on soma_temporary_operations to ",group,";")
      dbSendQuery(con, statement = command)
    }
  }
  if(task == "update"){
    dbWriteTable(con, "soma_temporary_operations",
                 tomo,
                 row.names = FALSE,
                 append = TRUE)
  }
}
