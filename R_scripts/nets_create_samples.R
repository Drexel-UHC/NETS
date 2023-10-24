library(dplyr)
library(data.table)
library(vroom)
library(stringr)

setwd("D:\\NETS\\NETS_2022\\ProcessedData\\Samples")

######## ADDRESS FIRST #############

# load in file with vroom
ptm <- proc.time()
address_first <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_AddressFirst.txt")
proc.time() - ptm
# takes about 4 mins
# n = 87,564,680 

# list column names and type
spec(address_first)

# create a reproduceable sample of 1000 records
set.seed(100)
sample_rows <- sample(1:nrow(address_first), 1000)
address_first_sample <- address_first[sample_rows, ]
address_first_sample <- address_first_sample %>% arrange(DunsNumber)

# create file with n=1000 sample of sales 
write.table(address_first_sample, file = "address_first_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM THIS DOES NOT FREE RAM. Use gc() or .rs.restartR()
rm(address_first)
gc()
.rs.restartR()

######## COMPANY ###################

ptm <- proc.time()
company <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_Company.txt")
proc.time() - ptm
# takes about 6 mins
# n = 87,564,680

spec(company)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

company_sample <- filter(company, DunsNumber %in% address_first_sample$DunsNumber)


setequal(address_first_sample$DunsNumber, company_sample$DunsNumber)

write.table(company_sample, file = "company_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(company)
gc()
# .rs.restartR()
######## EMP #######################

ptm <- proc.time()
emp <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_Emp.txt", delim='\t')
proc.time() - ptm
# takes about 6 mins
# n = 87,564,680

spec(emp)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

emp_sample <- filter(emp, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, emp_sample$DunsNumber)

write.table(emp_sample, file = "emp_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(emp)
gc()
# .rs.restartR()
######## MISC ######################

ptm <- proc.time()
misc <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_Misc.txt")
proc.time() - ptm
# takes about 5 mins
# n = 71,498,225

spec(misc)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

misc_sample <- filter(misc, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, misc_sample$DunsNumber)

write.table(misc_sample, file = "misc_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(misc)
gc()

######## MOVE ######################

ptm <- proc.time()
move <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_Move.txt")
proc.time() - ptm
# takes about 3 seconds
# n = 6,968,643

spec(move)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

# create sample where DunsNumbers match those of address_first sample
move_sample <- filter(move, DunsNumber %in% address_first_sample$DunsNumber)

# check if all DunsNumber values match 
setequal(address_first_sample$DunsNumber, move_sample$DunsNumber)

write.table(move_sample, file = "move_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(move)
gc()

######## MOVE SUMMARY ##############

ptm <- proc.time()
move_summary <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2022_MoveSummary.txt")
proc.time() - ptm
# takes about 2 seconds
# n = 5,766,810

spec(move_summary)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

# create sample where DunsNumbers match those of address_first sample
move_summary_sample <- filter(move_summary, DunsNumber %in% address_first_sample$DunsNumber)

# check if all DunsNumber values match 
setequal(address_first_sample$DunsNumber, move_summary_sample$DunsNumber)

write.table(move_summary_sample, file = "move_summary_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(move_summary)
gc()

######## SALES #####################

ptm <- proc.time()
sales <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_Sales.txt")
proc.time() - ptm
# takes about 6 mins
# n = 71,498,225

spec(sales)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

sales_sample <- filter(sales, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, sales_sample$DunsNumber)

write.table(sales_sample, file = "sales_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(sales)
gc()

######## SIC #######################

ptm <- proc.time()
sic <- vroom("Z:\\UHC_Data\\NETS_UHC\\NETS2022\\Raw Data\\ASCII\\NETS2022_SIC.txt", col_types = c(.default = "c"))
proc.time() - ptm
# takes about 5 mins
# n = 71,498,225
# set all column data types to character

spec(sic)

address_first_sample <- read.table("address_first_sample.txt", 
                                   header = TRUE, 
                                   colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

sic_sample <- filter(sic, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, sic_sample$DunsNumber)

write.table(sic_sample, file = "sic_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(sic)
gc()


