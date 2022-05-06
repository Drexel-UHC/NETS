library(dplyr)
library(data.table)
library(vroom)
library(stringr)

######## ADDRESS FIRST #############


# load in file with vroom
ptm <- proc.time()
address_first <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_AddressFirst.txt")
proc.time() - ptm
# takes about 6 minutes
# n = 71,498,225 

# list column names and type
spec(address_first)

# create a reproduceable sample of 1000 records
set.seed(100)
sample_rows <- sample(1:nrow(address_first), 1000)
address_first_sample <- address_first[sample_rows, ]

# create file with n=1000 sample of sales 
write.table(address_first_sample, file = "samples/address_first_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# read in sample
address_first_sample <- read.table("samples/address_first_sample.txt", 
                       header = TRUE, 
                       colClasses = c("character", "character", "character", "character", "character", "character", "character", "character"))

# remove large file to free up RAM THIS DOES NOT FREE RAM. Use gc() or broom icon in environment tab
rm(address_first)

######## COMPANY ###################

ptm <- proc.time()
company <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_Company.txt")
proc.time() - ptm
# takes about 4 mins
# n = 71,498,225

spec(company)

company_sample <- filter(company, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, company_sample$DunsNumber)

write.table(company_sample, file = "samples/company_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(company)

######## EMP #######################

ptm <- proc.time()
emp <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_Emp.txt")
proc.time() - ptm
# takes about 6 mins
# n = 71,498,225

spec(emp)

emp_sample <- filter(emp, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, emp_sample$DunsNumber)

write.table(emp_sample, file = "samples/emp_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(emp)


######## FIPS ######################

ptm <- proc.time()
fips <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_FIPS.txt")
proc.time() - ptm
# takes about 2.5 mins
# n = 71,498,225

spec(fips)

fips_sample <- filter(fips, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, fips_sample$DunsNumber)

write.table(fips_sample, file = "samples/fips_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
gc()


######## HQ COMPANY ################

ptm <- proc.time()
hq_company <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_HQCompany.txt")
proc.time() - ptm
# takes about 5 mins
# n = 71,498,225

spec(hq_company)

hq_company_sample <- filter(hq_company, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, hq_company_sample$DunsNumber)

write.table(hq_company_sample, file = "samples/hq_company_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(hq_company)


######## HQS #######################

ptm <- proc.time()
hqs <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_HQs.txt")
proc.time() - ptm
# takes about 3 mins
# n = 71,498,225

spec(hqs)

hqs_sample <- filter(hqs, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, hqs_sample$DunsNumber)

write.table(hqs_sample, file = "samples/hqs_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(hqs)

 
######## MISC ######################

ptm <- proc.time()
misc <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_Misc.txt")
proc.time() - ptm
# takes about 5 mins
# n = 71,498,225

spec(misc)

misc_sample <- filter(misc, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, misc_sample$DunsNumber)

write.table(misc_sample, file = "samples/misc_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(misc)


######## MOVE ######################

ptm <- proc.time()
move <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_Move.txt")
proc.time() - ptm
# takes about 3 seconds
# n = 6,968,643

spec(move)

# create sample where DunsNumbers match those of address_first sample
move_sample <- filter(move, DunsNumber %in% address_first_sample$DunsNumber)

# check if all DunsNumber values match 
setequal(address_first_sample$DunsNumber, move_sample$DunsNumber)

write.table(move_sample, file = "samples/move_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(move)


######## MOVE SUMMARY ##############

ptm <- proc.time()
move_summary <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_MoveSummary.txt")
proc.time() - ptm
# takes about 2 seconds
# n = 5,766,810

spec(move_summary)

# create sample where DunsNumbers match those of address_first sample
move_summary_sample <- filter(move_summary, DunsNumber %in% address_first_sample$DunsNumber)

# check if all DunsNumber values match 
setequal(address_first_sample$DunsNumber, move_summary_sample$DunsNumber)

write.table(move_summary_sample, file = "samples/move_summary_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(move_summary)


######## NAICS19 ###################

# ptm <- proc.time()
# naics19 <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_NAICS19.txt")
# proc.time() - ptm
# # takes about 2 mins
# # n = 71,498,225
# 
# spec(naics19)
# 
# naics19_sample <- filter(naics19, DunsNumber %in% address_first_sample$DunsNumber)
# 
# setequal(address_first_sample$DunsNumber, naics19_sample$DunsNumber)
# 
# write.table(naics19_sample, file = "samples/naics19_sample.txt", sep = "\t", col.names = TRUE)
# 
# # remove large file to free up RAM
# rm(naics19)


######## RATINGS ###################

# ptm <- proc.time()
# ratings <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_Ratings.txt")
# proc.time() - ptm
# # takes about 3 mins
# # n = 71,498,225
# 
# spec(ratings)
# 
# ratings_sample <- filter(ratings, DunsNumber %in% address_first_sample$DunsNumber)
# 
# setequal(address_first_sample$DunsNumber, ratings_sample$DunsNumber)
# 
# write.table(ratings_sample, file = "samples/ratings_sample.txt", sep = "\t", col.names = TRUE)
# 
# # remove large file to free up RAM
# rm(ratings)


######## SALES #####################

ptm <- proc.time()
sales <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_Sales.txt")
proc.time() - ptm
# takes about 6 mins
# n = 71,498,225

spec(sales)

sales_sample <- filter(sales, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, sales_sample$DunsNumber)

write.table(sales_sample, file = "samples/sales_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(sales)


######## SIC #######################

ptm <- proc.time()
sic <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_SIC.txt", col_types = c(.default = "c"))
proc.time() - ptm
# takes about 5 mins
# n = 71,498,225
# set all column data types to character

spec(sic)

sic_sample <- filter(sic, DunsNumber %in% address_first_sample$DunsNumber)

setequal(address_first_sample$DunsNumber, sic_sample$DunsNumber)

write.table(sic_sample, file = "samples/sic_sample.txt", sep = "\t", col.names = TRUE, row.names = FALSE)

# remove large file to free up RAM
rm(sic)







