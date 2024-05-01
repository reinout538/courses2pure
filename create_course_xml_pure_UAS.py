#This script creates a Pure xml import file based on an export from the VU-course management system, UAS

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

from concurrent.futures import ThreadPoolExecutor, as_completed
import os, sys
import requests
import json
import csv
import math
import time
import datetime
import pandas as pd
from xml.etree import ElementTree
from xml.etree.ElementTree import (Element, SubElement, Comment, tostring)
from xml.dom import minidom
from config import*
from bs4 import BeautifulSoup
from xml.sax.saxutils import unescape

url_person = "https://research.vu.nl/ws/api/524/persons?"

file_dir = sys.path[0]
courses_file = 'modules.xlsx'
disclaims_file = 'disclaims.xlsx'
opl_file = 'opleidingen_2023.xlsx'
xml_file = 'xml_pure_courses_p.xml'

pure_scopus_ids = []
ext_person_list = []
int_person_list = []
int_person_dict = {}
scopus_id2affil = {}
int_person_dict_vunet = {}
vunetid_list = []

acad_year = input("enter acad year - eg 2023-2024: ")
start_acad_yr = datetime.datetime(int(acad_year[0:4]), 9, 1)
start_vak = f"1-9-{acad_year[0:4]}"
eind_vak = f"31-8-{int(acad_year[0:4])+1}"
    
def get_pure_internal_persons():
        
    def get_response(offset, size):
        try:
            
            response = requests.get(url_person, headers={'Accept': 'application/json'},params={'size': size, 'offset':offset, 'apiKey':key_pure})
            
            for count,item in enumerate(response.json()['items'][0:]):  
                    
                    count_scopus=1

                    youshare_candidate = "false"
                    person_scopus_ids = []
                    person_affil_list = []
                    affil_first_dt = datetime.datetime(9999, 12, 31)
                    affil_last_dt = datetime.datetime(1900, 1, 1)


                    #get affiliations
                    for affil in item['staffOrganisationAssociations']:
                            affil_start_dt = datetime.datetime.strptime(affil['period']['startDate'][:10], '%Y-%m-%d')
                            if 'endDate' in affil['period']:
                                affil_end_dt = datetime.datetime.strptime(affil['period']['endDate'][:10], '%Y-%m-%d')
                            else: affil_end_dt = datetime.datetime(9999, 12, 31)
                            if 'jobTitle' in affil:
                                job_title = affil['jobTitle']['uri'][affil['jobTitle']['uri'].rfind("/")+1:]
                            else: job_title = ''
                            if 'emails' in affil:
                                email = affil['emails'][0]['value']['value']
                            else: email = ''
                            person_affil_list.append({'af_id':affil['pureId'],'af_org_id':affil['organisationalUnit']['uuid'],'af_source_id':affil['organisationalUnit']['externalId'], 'af_start':affil_start_dt,'af_end':affil_end_dt, 'job_title':job_title,'e_mail':email})
                            if affil_start_dt < affil_first_dt:
                                affil_first_dt = affil_start_dt
                            if affil_end_dt > affil_last_dt:
                                affil_last_dt = affil_end_dt

                    #get scopus-IDs
                    if 'ids' in item:
                        for ct, extid in enumerate (item['ids']):
                            if item['ids'][ct]['type']['term']['text'][0]['value'] == 'Scopus Author ID':
                                person_scopus_ids.append(item['ids'][ct]['value']['value'])
                                pure_scopus_ids.append(item['ids'][ct]['value']['value'])
                                #create index scopus-ID + affiliation_list
                                scopus_id2affil[item['ids'][ct]['value']['value']] = person_affil_list
                                count_scopus += 1    

                    if 'keywordGroups' in item:
                        for keyword_group in item['keywordGroups']:
                            if keyword_group['logicalName'] =="/dk/atira/pure/keywords/You_Share_Participant":
                                youshare_candidate = "true"
                            else:
                                youshare_candidate = "false"

                    int_person_list.append({'person_uuid':item['uuid'],'youshare':youshare_candidate,'scopus_ids':person_scopus_ids,'personaffiliations':person_affil_list, 'affil_first_dt': affil_first_dt, 'affil_last_dt': affil_last_dt})                         
                    int_person_dict[item['uuid']] = {'person_uuid':item['uuid'],'youshare':youshare_candidate,'scopus_ids':person_scopus_ids,'personaffiliations':person_affil_list, 'affil_first_dt': affil_first_dt, 'affil_last_dt': affil_last_dt}
                    if 'externalId' in item:
                        vunetid_list.append(item['externalId'])
                        int_person_dict_vunet[item['externalId']] = {'person_uuid':item['uuid'],'youshare':youshare_candidate,'scopus_ids':person_scopus_ids,'personaffiliations':person_affil_list, 'affil_first_dt': affil_first_dt, 'affil_last_dt': affil_last_dt}
            
            return int_person_list
        except requests.exceptions.RequestException as e:
            return e
     
    def runner():
        size = 1000
        offset = 0
        response = requests.get(url_person, headers={'Accept': 'application/json'},params={ 'apiKey':key_pure})
        no_records = (response.json()['count'])
        cycles = (math.ceil(no_records/size))
        print (f"getting {no_records} person records from Pure in {cycles} cycles")
        
        threads= []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for request in range (cycles)[0:]:
                threads.append(executor.submit(get_response, offset, size))
                offset += size
                
            for task in as_completed(threads):
                print (f"got {len(int_person_list)} of {no_records} records")
                #clear_output('wait') 
                
    runner()

#main
get_pure_internal_persons()

with open(os.path.join(file_dir,'vunetids.txt'), 'w') as fp:
    fp.write('\n'.join(vunetid_list))

#dataframes
#read excel courses
df_vakken = pd.read_excel(os.path.join(file_dir,courses_file), sheet_name='Sheet1', skiprows=[1], converters={'Extern ID':str})
#read disclaims
df_disclaims = pd.read_excel(os.path.join(file_dir,disclaims_file), sheet_name='Sheet1')


#log
df_log = pd.DataFrame(columns=['course_code', 'course_id', 'credits', 'record added','reason rejected', 'URL-status', 'has_developer', 'has_lecturer', ' has_degree', 'title'])

#class_velden = {"Lange naam": "t", "Opmerkingen" : "009", "Doel" : "002", "Inhoud" : "003", "Werkwijze" : "004", "Literatuur" : "006", "Toetsing" : "005", "Doelgroep" : "007", "Vereiste voorkennis" : "010", "Aanbevolen voorkennis" : "011", "Intekenprocedure" : "008", "Uitleg in Canvas" : "012", "Globaal doel KPI" : ""}
class_velden = {"Additional Information" : "009", "Additional Information Target Audience" : "007", "Additional Information Teaching Methods" : "004", "Course Content" : "003", "Course Objective" : "002", "Custom Course Registration" : "008", "Entry Requirements" : "010", "Explanation Canvas" : "012", "Literature" : "006", "Method of Assessment" : "005", "Recommended background knowledge" : "011"}
class_taal = {"Engels (EN)" : "english", "Nederlands (NL)" : "dutch", "Tweetalig (Z1)": "bilingual", "Spaans (ES)": "spanish"}
class_period = {"Ac. Jaar (september)" : "1", "Ac. Jaar (februari)" : "2", "Semester 1" : "20", "Semester 2" : "30", "Periode 1" : "110", "Periode 1+2" : "111", "Periode 1+2+3+4+5" : "113", "Periode 1+2+3" : "114", "Periode 1+2+3+4" : "115", "Periode 2" : "120", "Periode 2+3" : "121", "Periode 2+3+4" : "122", "Periode 2+3+4+5" : "123", "Periode 2+3+4+5+6" : "124", "Periode 3" : "130", "Periode 3+4" : "131", "Periode 3+4+5" : "132", "Periode 3+4+5+6" : "134", "Periode 4" : "140", "Periode 4+5" : "141", "Periode 4+5+6" : "142", "Periode 5" : "150", "Periode 5+6" : "151", "Periode 6" : "160", "Periode 7" : "170"}
class_study = {"B" : "bachelor", "M" : "master", "P" : "premaster", "PG" : "postgraduate"}

#xml envelope
xml_courses = Element('v1:courses', {'xmlns:v1':"v1.course.pure.atira.dk", "xmlns:v3":"v3.commons.pure.atira.dk"})

courses_processed = []

#process vakken
for index_no in df_vakken.index[0:]:

    devel_associated = []
    lect_associated = []
    study_list = []
    field_ct = developer_ct = lecturer_ct = 0
    reason_rejection = ""
    record_added = has_developer = has_lecturer = has_degree = "true"
    text_fields = []

    #get course data
    vak_code = str(df_vakken['Extern ID'][index_no])
    vak_id = str(df_vakken['Code'][index_no])
    acad_periodes = df_vakken['Aangeboden periodes'][index_no]
    vak_oms_en = df_vakken['Lange naam (studiegids, diplomasupplement, cijferlijst) (EN)'][index_no]
    vak_oms_nl = df_vakken['Lange naam (studiegids, diplomasupplement, cijferlijst) (NL)'][index_no]
    niveau = df_vakken['Niveau'][index_no]
    taal = df_vakken['Onderwijstaal'][index_no]
    credit = df_vakken['Studiepunten (EC) optimum'][index_no]
    lecturers = df_vakken["Docent(en) (id)"][index_no]
    developers = df_vakken["Vakcoordinator (id)"][index_no]
    developer_repl = df_vakken["vervangend vakcoordinator (id)"][index_no]
    opl_codes = df_vakken["Bijbehorende opleidingscodes"][index_no]
    werkvorm_codes = df_vakken["Werkvormen (code)"][index_no]
    
    if pd.isna (opl_codes) == False:
        opl_list = opl_codes.split(",")
        for code in opl_list:
            code = code.strip()
            study_label = (code[1:code.find("_")])
            try:
                study_class = class_study[study_label]
                if study_class in study_list:
                    continue
                else:
                    study_list.append(study_class)
            except:
                continue
                
    #check URL in study guide and scrape text fields
    url_study_guide = f"https://studiegids.vu.nl/en/courses/{acad_year}/{vak_id}"
    try:
        check_guide = requests.get(url_study_guide)
        if check_guide.status_code == 404:
            print (check_guide.status_code)
            reason_rejection = 'not in study guide'
            record_added = 'false'
            df_log.loc[len(df_log.index)] = [vak_code, vak_id, credit, record_added, reason_rejection, check_guide.status_code, has_developer, has_lecturer, has_degree,'']
            continue
        else:
            print (check_guide.status_code)
            #scrape text fields study guide
            S = BeautifulSoup(check_guide.text, 'html.parser')
            for descr in S.find(id="course-description").find_all('div', class_="paragraph"):
                descr_header = descr.find('h3').string
                #descr_text = str(descr).replace("<br/>", " ").replace('<div class="paragraph">','').replace('</div>','').replace(f"<h3>{descr_header}</h3>",'').strip()
                descr_text = BeautifulSoup(str(descr).replace("<br/>", " ").replace("</p><p>", " "), 'html.parser').get_text().replace(descr_header, '').strip()
                """
                #remove html tags except </br>
                descr_text = BeautifulSoup(descr, 'html.parser')
                for e in descr_text.find_all():
                    if e.name not in ['br']:
                        e.unwrap()
                """
                text_fields.append({'type':class_velden[descr_header],'text' : descr_text})
                
    except:
        print (check_guide.status_code)
        reason_rejection = 'url-check failed'
        record_added = 'false'
        df_log.loc[len(df_log.index)] = [vak_code, vak_id, credit, record_added, reason_rejection, check_guide.status_code, has_developer, has_lecturer, has_degree,'']
        continue

    print (vak_code, vak_id, index_no, len(df_vakken.index))

    #process acad period
    if pd.isna(acad_periodes) == False:
        period_list = list(acad_periodes.split(","))
    else:
        reason_rejection = 'no period'
        record_added = 'false'
        df_log.loc[len(df_log.index)] = [vak_code, vak_id, credit, record_added, reason_rejection, check_guide.status_code, has_developer, has_lecturer, has_degree, course_title]
        continue

    #set title
    if taal == "Nederlands (NL)":
        if pd.isna(vak_oms_nl) == False:
            course_title = vak_oms_nl
        else:
            course_title = vak_oms_en
    else:
        if pd.isna(vak_oms_en) == False:
            course_title = vak_oms_en
        else:
            course_title = vak_oms_nl

    #set language
    if pd.isna(taal) == False:
        key_language = class_taal[taal]
    else:
        key_language = 'unknown'

    #create contributor lists - if there are no lecturers, the coordinator is listed as lecturer

    #developers
    devel_disclaimed = df_disclaims.loc[df_disclaims['courseID'] == vak_id,'vunetID']
    
    if pd.isna(developers) == True:
        developer_list = []
    else:
        developers = developers.replace(" ","")
        developer_list = list(developers.split(","))
        
        #add replacement coordinator
        if developer_repl != None:
            developer_list.append (developer_repl)
        
        #remove disclaimed
        for developer in developer_list:
            if developer in list(devel_disclaimed.values):
                developer_list.remove(developer)
            else:
                continue
        
    #lecturers
    if pd.isna(lecturers) == True:
        lecturer_list = developer_list
    else:
        lecturers = lecturers.replace(" ","")
        lecturer_list = list(lecturers.split(","))

    
    #loop through developer_list and match with Pure persons and affiliations
    for vunetid in developer_list:

        if vunetid not in vunetid_list:
            continue
        
        person_org_list = []
        person_role = 'Is verantwoordelijk voor'
        
        #loop through Pure affiliatons and get current
        for affil in int_person_dict_vunet[vunetid]['personaffiliations']:
            if affil['af_end'] > start_acad_yr:
                person_org_list.append(affil['af_org_id'])
            else:
                continue
            
        #if no current affiliations found    
        if person_org_list == []:
            #loop through Pure affiliatons and get most recent
            affil_last_dt = datetime.datetime(1900, 1, 1)
            for affil in int_person_dict_vunet[vunetid]['personaffiliations']:
                if affil['af_end'] > affil_last_dt:
                    affil_last_dt = affil['af_end']
                    person_org_list = [affil['af_org_id']]
                else:
                    continue

        #add person dict to list of persons associated
        devel_associated.append({'id':vunetid, 'role':person_role, 'affil':person_org_list})
        
    #loop through lecturer_list and match with Pure persons and affiliations
    
    for vunetid in lecturer_list:

        if vunetid not in vunetid_list:
            continue
        
        person_org_list = []
        person_role = 'Docent'
        
        #loop through Pure affiliatons and get current
        for affil in int_person_dict_vunet[vunetid]['personaffiliations']:
            if affil['af_end'] > start_acad_yr:
                person_org_list.append(affil['af_org_id'])
            else:
                continue
            
        #if no current affiliations found    
        if person_org_list == []:
            #loop through Pure affiliatons and get most recent
            affil_last_dt = datetime.datetime(1900, 1, 1)
            for affil in int_person_dict_vunet[vunetid]['personaffiliations']:
                if affil['af_end'] > affil_last_dt:
                    affil_last_dt = affil['af_end']
                    person_org_list = [affil['af_org_id']]
                else:
                    continue

        #add person dict to list of persons associated
        lect_associated.append({'id':vunetid, 'role':person_role, 'affil':person_org_list})
        
    if lect_associated == []:
        has_lecturer = 'false'
        reason_rejection = 'no lecturer in pure'
        record_added = 'false'
        df_log.loc[len(df_log.index)] = [vak_code, vak_id, credit, record_added, reason_rejection, check_guide.status_code, has_developer, has_lecturer, has_degree, course_title]
        continue
           
    #build xml course body
    course_record = SubElement(xml_courses,'v1:course', id=f"{vak_code}", managedInPure="false", type="course")
    title = SubElement(course_record, 'v1:title')
    title.text = str(course_title)
    addit_descr = SubElement(course_record, 'v1:additionalDescriptions')
    course_ids = SubElement(course_record, 'v1:ids')
    course_id = SubElement(course_ids, 'v3:id', type="course")
    course_id.text = vak_id
    course_start = SubElement(course_record, 'v1:startDate')
    course_start.text = str(start_vak)
    course_end = SubElement(course_record, 'v1:endDate')
    course_end.text = str(eind_vak)
    if pd.isna(credit) == False:
        course_level = SubElement(course_record, 'v1:level')
        course_level.text = f"{str(math.ceil(credit))}_00_ec"
    developers = SubElement(course_record, 'v1:developers')
    managing_org = SubElement(course_record, 'v1:managingOrganisation', lookupId='xxxxxxxx')
    occurrences = SubElement(course_record, 'v1:occurrences')

    #keywords
    if pd.isna(taal) == False and opl_list != []:
        keywords = SubElement(course_record, 'v1:keywords')
    
    if pd.isna(taal) == False:    
        keyw_language = SubElement(keywords, 'v1:keyword', logicalName="/dk/atira/pure/keywords/language_course", key=key_language)

    if study_list != []:
        for study_class in study_list:
            keyw_study = SubElement(keywords, 'v1:keyword', logicalName="/dk/atira/pure/keywords/courses/studytype", key=study_class)
        
    #add developer contribution to xml
    if devel_associated != []:
        for person in devel_associated:
            developer = SubElement(developers, 'v1:developer', id=f"{vak_code}_{person['id']}")
            dev_person = SubElement(developer, 'v1:person', lookupId = str(person['id']))
            dev_orgs = SubElement(developer, 'v1:organisations')    
            if developer_ct == 0:
                dev_orgs_dir = SubElement(course_record, 'v1:organisations')
            else:
                pass
            
            for org in person['affil']:
                dev_org = SubElement(dev_orgs, 'v1:organisation', lookupId = org)
                dev_org_dir = SubElement(dev_orgs_dir, 'v1:organisation', lookupId = org)

            developer_ct += 1
    else:
        course_record.remove(developers)
        dev_orgs = SubElement(course_record, 'v1:organisations')
        dev_org = SubElement(dev_orgs, 'v1:organisation', lookupId = lect_associated[0]['affil'][0])
        has_developer = "false"
    
    #add occurances to xml
    for period in period_list:

        period = period.strip()
        acad_periode = class_period[period]
    
        occurrence = SubElement(occurrences, 'v1:occurrence', id = f"{vak_code}_{str(acad_year[0:4])}_{acad_periode}")
        #occurrence = SubElement(occurrences, 'v1:occurrence', id = f"{vak_code}_2022_{acad_periode}")
        occurrence_semester = SubElement(occurrence, 'v1:semester')
        occurrence_semester.text = str(acad_periode)
        occurrence_year = SubElement(occurrence, 'v1:year')
        occurrence_year.text = str(acad_year[0:4])
        lecturers = SubElement(occurrence, 'v1:lecturers')

        #find lecturers in person list
        for person in lect_associated:
            
            lecturer = SubElement(lecturers, 'v1:lecturer', id= f"{vak_code}_{acad_periode}_{person['id']}", role = 'teacher')
            lect_person = SubElement(lecturer, 'v1:person', lookupId = str(person['id']))
            lect_orgs = SubElement(lecturer, 'v1:organisations')
            for org in person['affil']:
                lect_org = SubElement(lect_orgs, 'v1:organisation', lookupId = org)

            lecturer_ct += 1

        #occurance requires separate contributing organisation - just take last one
        occur_orgs = SubElement(occurrence, 'v1:organisations')
        if lect_associated != []:
            occur_org = SubElement(occur_orgs, 'v1:organisation', lookupId = lect_associated[0]['affil'][0])
        else:
            occur_org = SubElement(occur_orgs, 'v1:organisation', lookupId = "xxxxxxxx")

    #add descriptive fields
    
    #url study guide    
    descr_field = SubElement (addit_descr, 'v1:description', type='001')
    descr_field.text = f"<![CDATA[<html><head><meta http-equiv='content-type' content='text/html; charset=windows-1252'></head><body><a href='{url_study_guide}'>{url_study_guide}</a></body></html>]]>"

    #text field

    for field in text_fields:

        #text_field = add_missing_spaces(field['text'])
        text_field = field['text']
        
        if field['text'] != None:
            descr_field = SubElement (addit_descr, 'v1:description', type=field['type'])
            descr_field.text = unescape(f"<![CDATA[<html><head><meta http-equiv='content-type' content='text/html; charset=windows-1252'></head><body>{text_field.replace('•','<br>•').replace('- ','<br>- ').replace('* ','<br>- ')}</body></html>]]>")
            #descr_field.text = f"<![CDATA[<html><head><meta http-equiv='content-type' content='text/html; charset=windows-1252'></head><body>{text_field}</body></html>]]>"
       
    df_log.loc[len(df_log.index)] = [vak_code, vak_id, credit, record_added, reason_rejection, check_guide.status_code, has_developer, has_lecturer, has_degree, course_title]
    
xmlstr = minidom.parseString(tostring(xml_courses)).toprettyxml(indent="   ")
                                                                                                                           
xml_file = open(os.path.join(file_dir, xml_file), "w", encoding = 'UTF-8')
xml_file.write(xmlstr)
xml_file.close()

df_log.to_csv(os.path.join(file_dir, "operations_log.csv"), encoding='utf-8', index = False)

#print (xmlstr)
