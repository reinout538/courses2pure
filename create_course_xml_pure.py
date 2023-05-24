#TO DO
#Opleiding toevoegen
#CDATA wegschrijven vanuit script (nu met zoek/vervang < en > achteraf in notepad++)
#Selectie Engelse veldomschrijving indien in 2 talen (ipv eerste)

from concurrent.futures import ThreadPoolExecutor, as_completed
import os, sys
import requests
import json
import csv
import math
import datetime
import pandas as pd
from xml.etree import ElementTree
from xml.etree.ElementTree import (Element, SubElement, Comment, tostring)
from xml.dom import minidom
from config import*

url_person = "https://research.accept.vu.nl/ws/api/524/persons?"

file_dir = sys.path[0]
courses_file = 'Pure Vakken_bewerkt.xlsx'
opl_file = 'opleidingen_2023.xlsx'
xml_file = 'xml_pure_courses.xml'

pure_scopus_ids = []
ext_person_list = []
int_person_list = []
int_person_dict = {}
scopus_id2affil = {}
int_person_dict_vunet = {}
vunetid_list = []

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

#dataframes
#read excel courses
df_vakken = pd.read_excel(os.path.join(file_dir,courses_file), sheet_name='Vakken', skiprows=[1])
df_organisatie = pd.read_excel(os.path.join(file_dir,courses_file), sheet_name='Organisatie', skiprows=[1])
df_personen = pd.read_excel(os.path.join(file_dir,courses_file), sheet_name='Personen en rollen', skiprows=[1])
df_velden = pd.read_excel(os.path.join(file_dir,courses_file), sheet_name='Beschrijvende velden', skiprows=[1])
df_inhoud_velden = pd.read_excel(os.path.join(file_dir,courses_file), sheet_name='Inhoud beschrijvende velden', dtype={'Regel_Concat':str}, skiprows=[1])
#read excel programmes
#df_opleidingen = pd.read_excel(os.path.join(file_dir,opl_file), sheet_name='test')
#log
df_log = pd.DataFrame(columns=['course_code', 'course_id', 'credits', 'record added','reason rejected', 'URL-status', 'has_developer', 'has_lecturer', ' has_degree', 'title', 'categorie'])

class_velden = {"Lange naam": "t", "Opmerkingen" : "08", "Doel" : "01", "Inhoud" : "02", "Werkwijze" : "03", "Literatuur" : "05", "Toetsing" : "04", "Doelgroep" : "06", "Vereiste voorkennis" : "09", "Aanbevolen voorkennis" : "10", "Intekenprocedure" : "07", "Uitleg in Canvas" : "11", "Globaal doel KPI" : ""}
class_taal = {"EN" : "english", "NL" : "dutch", "Tweetalig": "bilingual"}

#xml envelope
xml_courses = Element('v1:courses', {'xmlns:v1':"v1.course.pure.atira.dk", "xmlns:v3":"v3.commons.pure.atira.dk"})

courses_processed = []

#process vakken
for index_no in df_vakken.index[0:]:

    persons_assoc = []
    field_ct = developer_ct = lecturer_ct = 0
    reason_rejection = ""
    record_added = has_developer = has_lecturer = has_degree = "true"
            
    vak_code = df_vakken['Unieke vakcode'][index_no]

    #skip rows with course ID that was already processed - semesters will be accmulated in occurances
    if vak_code in courses_processed:
        continue
    courses_processed.append(vak_code)
        
    vak_id = df_vakken['Vak ID'][index_no]
    vak_oms_en = df_vakken['Vak oms (EN)'][index_no]
    vak_oms_nl = df_vakken['Vak oms (NL)'][index_no]
    acad_jaar = df_vakken['Acad. Jaar'][index_no]
    #acad_periode = df_vakken['Acad. Per'][index_no]
    acad_periodes = df_vakken.loc[df_vakken['Unieke vakcode'] == vak_code]
    graad = df_vakken['Moeilijkheidsgraad'][index_no]
    categorie = df_vakken['Categorie'][index_no]
    taal = df_vakken['Taal'][index_no]
    credit = math.ceil(df_vakken['Opt. Credits'][index_no])
    start_vak = df_vakken['Startdatum vak'][index_no]
    
    print (index_no, vak_code)
    
    #set title
    if pd.isna(vak_oms_en) == False:
        course_title = vak_oms_en
    else:
        course_title = vak_oms_nl

    #check URL in study guide
    url_study_guide = "https://studiegids.vu.nl/en/courses/2022-2023/"+vak_id
    check_guide = requests.get(url_study_guide)
    if check_guide.status_code == 404:
        continue
    """
    #find programma via study guide url generation
    for opl_index in df_opleidingen.index[0:]:
        progr_id = df_opleidingen['_ - StudyOverview - _ - id'][opl_index]
        progr_study = df_opleidingen['_ - StudyOverview - _ - degree'][opl_index]
        print (progr_id, progr_study)
        url_course_progr = f"https://studiegids.vu.nl/nl/{progr_study}/2022-2023/{progr_id}/{vak_id}"
        check_progr_url = requests.get(url_course_progr)
        print (url_course_progr, check_progr_url.status_code)
    """
    
    #build xml course body
    course_record = SubElement(xml_courses,'v1:course', id=f"{vak_code}", managedInPure="false", type="course")
    title = SubElement(course_record, 'v1:title')
    title.text = str(course_title)
    addit_descr = SubElement(course_record, 'v1:additionalDescriptions')
    course_ids = SubElement(course_record, 'v1:ids')
    course_id = SubElement(course_ids, 'v3:id', type="course")
    course_id.text = vak_id
    course_start = SubElement(course_record, 'v1:startDate')
    course_start.text = str(start_vak.replace('.', '-'))
    course_level = SubElement(course_record, 'v1:level')
    course_level.text = f"{str(credit)}_00_ec"
    developers = SubElement(course_record, 'v1:developers')
    managing_org = SubElement(course_record, 'v1:managingOrganisation', lookupId='xxxxxxxx')
    occurrences = SubElement(course_record, 'v1:occurrences')
    
    if pd.isna(taal) == False:
        keywords = SubElement(course_record, 'v1:keywords')
        keyw_language = SubElement(keywords, 'v1:keyword', logicalName="/dk/atira/pure/keywords/language_course", key=class_taal[taal])
    
    #get org by vak_code
    organisaties = df_organisatie.loc[df_organisatie['Unieke vakcode'] == vak_code]

    for row_label, row in organisaties.iterrows():
        if row['Relatie'] == "Wordt aangeboden door":
            org_code = row['Organisatie ID']
        else: pass
        
    #get persons by vak_code
    personen = df_personen.loc[df_personen['Unieke vakcode'] == vak_code]
    
    #loop through person records in df and match with Pure persons and affiliations
    for row_label, row in personen.iterrows():

        person_org_list = []

        person_role = row['Type relatie']
        get_person_id = row['VUNET-ID']

        if pd.isna(get_person_id) == True:
            continue
        person_id = get_person_id.lower()
        if person_id not in vunetid_list:
            continue
        
        #loop through Pure affiliatons and get current
        for affil in int_person_dict_vunet[person_id]['personaffiliations']:
            if affil['af_end'].year >= int(2022):
                person_org_list.append(affil['af_org_id'])
            else:
                continue
            
        #if no current affiliations found    
        if person_org_list == []:
            #loop through Pure affiliatons and get most recent
            affil_last_dt = datetime.datetime(1900, 1, 1)
            for affil in int_person_dict_vunet[person_id]['personaffiliations']:
                if affil['af_end'] > affil_last_dt:
                    affil_last_dt = affil['af_end']
                    person_org_list = [affil['af_org_id']]
                else:
                    continue

        #add person dict to list of persons associated
        persons_assoc.append({'id':person_id, 'role':person_role, 'affil':person_org_list})

    #add developer contribution to xml
    for person in persons_assoc:
        if person['role'] == 'Is verantwoordelijk voor':
            developer = SubElement(developers, 'v1:developer', id=f"{vak_code}_{person['id']}")
            dev_person = SubElement(developer, 'v1:person', lookupId = str(person['id']))
            if developer_ct == 0:
                dev_orgs = SubElement(course_record, 'v1:organisations')    
            else:
                pass
            
            for org in person['affil']:
                dev_org = SubElement(dev_orgs, 'v1:organisation', lookupId = org)

            developer_ct += 1
        
    #add occurances to xml
    for row_label, row in acad_periodes.iterrows():
        lecturer_list = []
        acad_periode = row['Acad. Per']
    
        occurrence = SubElement(occurrences, 'v1:occurrence', id = f"{vak_code}_{acad_jaar}_{acad_periode}")
        occurrence_semester = SubElement(occurrence, 'v1:semester')
        occurrence_semester.text = str(acad_periode)
        occurrence_year = SubElement(occurrence, 'v1:year')
        occurrence_year.text = str(acad_jaar)
        lecturers = SubElement(occurrence, 'v1:lecturers')

        #find lecturers in person list
        for person in persons_assoc:
            if person['role'] == 'Docent' or 'Examinator':
                if person['id'] in lecturer_list:
                    continue
                lecturer_list.append(person['id'])
                lecturer = SubElement(lecturers, 'v1:lecturer', id= f"{vak_code}_{acad_periode}_{person['id']}", role = 'teacher')
                lect_person = SubElement(lecturer, 'v1:person', lookupId = str(person['id']))
                lect_orgs = SubElement(lecturer, 'v1:organisations')
                for org in person['affil']:
                    lect_org = SubElement(lect_orgs, 'v1:organisation', lookupId = org)

                lecturer_ct += 1
    
        #occurance requires separate contributing organisation - just take last one
        occur_orgs = SubElement(occurrence, 'v1:organisations')
        if persons_assoc != []:
            occur_org = SubElement(occur_orgs, 'v1:organisation', lookupId = persons_assoc[0]['affil'][0])
        else:
            occur_org = SubElement(occur_orgs, 'v1:organisation', lookupId = "xxxxxxxx")
        

    if developer_ct == 0:
        course_record.remove(developers)
        dev_orgs = SubElement(course_record, 'v1:organisations')
        dev_org = SubElement(dev_orgs, 'v1:organisation', lookupId = org)
        has_developer = "false"
    
    #get fields by vak_code
    velden = df_velden.loc[df_velden['Unieke vakcode'] == vak_code]

    #fields may be in both EN and NL - in that case EN is preferred
    veld_types_processed = []
    
    for row_label, row in velden.iterrows():
        
        #print (row['Unieke vakcode'], row['Subtype'], row['Taal'], row['Referentie'])
        veld_type = class_velden[row['Subtype']]
        if veld_type in veld_types_processed:
            continue
        else:
            veld_types_processed.append(veld_type)
        
        veld_referentie = row['Referentie']
        
        #get field content
        velden_inhoud = df_inhoud_velden.loc[df_inhoud_velden['Referentie'] == veld_referentie]
        veld_text = ""
        
        if veld_type != "":

            """
            #print (velden)
            rows_subtype = velden.loc[velden['Subtype'] == row['Subtype']]
            has_EN = rows_subtype.loc[rows_subtype['Taal'] == 'EN']
            print (has_EN)
            """

            for row_label_inh, row_inh in velden_inhoud.iterrows():
                #print (row_inh['Regel '])
                if pd.isna(row_inh['Regel ']) == True:
                    continue
                veld_text = f"{veld_text}{str(row_inh['Regel_Concat'])} "
                #print (row_inh['Referentie'], row_inh['Volgnummer'], row_inh['Regel '])

            if veld_type == 't':
                #overwrite course title with long name
                title.text = veld_text.strip()
            else:
                #add decriptive as field to xml
                descr_field = SubElement (addit_descr, 'v1:description', type=veld_type)
                #remove excess spaces
                descr_field_clean = " ".join(veld_text.split())
                descr_field.text = f"<![CDATA[<html><head><meta http-equiv='content-type' content='text/html; charset=windows-1252'></head><body>{descr_field_clean}</body></html>]]>"
                field_ct += 1
        
    if check_guide.status_code == 200:
        descr_field = SubElement (addit_descr, 'v1:description', type='00')
        descr_field.text = f"<![CDATA[<html><head><meta http-equiv='content-type' content='text/html; charset=windows-1252'></head><body><a href='{url_study_guide}' target='_blank'>{url_study_guide}</a></body></html>]]>"
        field_ct += 1
    else:
        pass

    if field_ct == 0:
        course_record.remove(addit_descr)

    if lecturer_ct == 0:
        xml_courses.remove(course_record)
        record_added = "false"
        has_lecturer = "false"
        #reason_rejection = "no lecturers"


    df_log.loc[len(df_log.index)] = [vak_code, vak_id, credit, record_added, reason_rejection, check_guide.status_code, has_developer, has_lecturer, has_degree, course_title, categorie]

xmlstr = minidom.parseString(tostring(xml_courses)).toprettyxml(indent="   ")
                                                                                                                           
xml_file = open(os.path.join(file_dir, xml_file), "w", encoding = 'UTF-8')
xml_file.write(xmlstr)
xml_file.close()

df_log.to_csv(os.path.join(file_dir, "operations_log.csv"), encoding='utf-8', index = False)

#print (xmlstr)
