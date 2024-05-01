# courses2pure
#This script creates a Pure xml import file based on an export from the VU course-management system, UAS

#GET Pure internal person records
#create dataframe based on UAS-export (modules)
#loop through courses in dataframe
    #extract course metadata from df
    #check if url study guide (based on course id) exists - if not: continue with next course
    #scrape study guide text fields (missing spaces in UAS-export)
    #match vunetIDs involved staff in UAS against list of Pure internal persons to select person records and affiliations
    #create course xml and add to full xml
    #add log data for course record
#save xml-file
#save log-file
