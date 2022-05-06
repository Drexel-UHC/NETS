library(dplyr)
library(vroom)
library(stringr)

######## SIC #######################

ptm <- proc.time()
sic <- vroom("D:\\NETS\\NETS_2019\\RawData\\NETS2019_SIC.txt", col_types = c(.default = "c"))
proc.time() - ptm
# takes about 5 mins
# n = 71,498,225
# set all column data types to character


# filter rows that changed 3-digit SIC code at any point in time
sic_change <- sic %>%
  filter(str_detect("Yes", SICChange))

# FROM sic_change: filter rows with SIC codes that start with "581" (restaurant) in any year
sic_change_581 <- sic_change %>%
  filter_all(any_vars(str_starts(., "581")))

# FROM sic_change_581: filter rows with 3-digit SIC code (which are most recent codes) "581"
sic_change_to_581 <- sic_change_581 %>% 
  filter(str_detect(SIC3, "581"))


# FROM sic_change filter rows with SIC codes that start with "54" (food stores) in any year
sic_change_54 <- sic_change %>%
  filter_all(any_vars(str_starts(.,"54")))

# FROM sic_change_54 filter rows with SIC codes that did not change to code 54
sic_change_54_to_not54 <- sic_change_54 %>% 
  filter(str_starts(SIC3, "54", negate = TRUE))

# It is possible that I captured some dunsnumbers that changed 3-digit SIC codes 
#from one category to another and then back again (ex: SIC92 = 58111111, 
#SIC93 = 58922222, SIC94 = 58133333). These instances can only exist in the 
#"Other to 581" or "Other to 54 OR 54A to 54B" categories. 

#further detail in NETS notes and in "NETS workflow" email thread 3/11/22