library(dplyr)
library(tidyr)

address_first_sample <- read.table("samples/address_first_sample.txt", 
                                   header = TRUE, colClasses = "character")

company_sample <- read.table("samples/company_sample.txt", 
                         header = TRUE, colClasses = "character")

emp_sample <- read.table("samples/emp_sample.txt", 
                                   header = TRUE, colClasses = "character")

fips_sample <- read.table("samples/fips_sample.txt", 
                          header = TRUE, colClasses = "character")

hq_company_sample <- read.table("samples/hq_company_sample.txt", 
                         header = TRUE, colClasses = "character")

hqs_sample <- read.table("samples/hqs_sample.txt", 
                                   header = TRUE, colClasses = "character")

misc_sample <- read.table("samples/misc_sample.txt", 
                          header = TRUE, colClasses = "character")

move_sample <- read.table("samples/move_sample.txt", 
                                   header = TRUE, colClasses = "character")

move_summary_sample <- read.table("samples/move_summary_sample.txt", 
                                   header = TRUE, colClasses = "character")

naics19_sample <- read.table("samples/naics19_sample.txt", 
                           header = TRUE, colClasses = "character")

sales_sample <- read.table("samples/sales_sample.txt", 
                                   header = TRUE, colClasses = "character")

sic_sample <- read.table("samples/sic_sample.txt", 
                           header = TRUE, colClasses = "character")


######## NON-MOVE SAMPLE ###########

# join all sample dataframes except move, move_summary, naics, and ratings
# remove unnecessary columns


files<- list(address_first_sample, company_sample, emp_sample, hq_company_sample, hqs_sample, sales_sample, sic_sample)


non_move_sample <- files %>% reduce(left_join, by = "DunsNumber")


non_move_sample <- non_move_sample %>% 
  select(-c(Officer, Title, Area, Phone, HQArea, HQPhone))

write.table(sales_sample, file = "samples/non_move_sample.txt", sep = "\t", col.names = TRUE)






########SANDBOX############

# select columns with string matches to subset decades; include all columns with
#no years in each subset

ptm <- proc.time()
sales90 <- sales %>% 
  select(DunsNumber, matches('90|91|92|93|94|95|96|97|98|99'), SalesHere, SalesHereC, SalesGrowth, SalesGrowthPeer)

sales00 <- sales %>% 
  select(DunsNumber, matches('00|01|02|03|04|05|06|07|08|09'), SalesHere, SalesHereC, SalesGrowth, SalesGrowthPeer)

sales10 <- sales %>% 
  select(DunsNumber, matches('10|11|12|13|14|15|16|17|18|19'), SalesHere, SalesHereC, SalesGrowth, SalesGrowthPeer)
proc.time() - ptm



# takes too long/too much RAM to run:
# write.table(sales90, file = "sales90.txt", sep = "\t", col.names = TRUE)

ptm <- proc.time()
saveRDS(sales90, "sales90.Rds")
proc.time() - ptm

ptm <- proc.time()
sales90_rds <- readRDS("sales90.Rds")
proc.time() - ptm

identical(sales90, sales90_rds)
