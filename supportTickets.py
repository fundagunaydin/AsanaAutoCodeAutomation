import json
import asana
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import time
import datetime
import pyodbc



#global variables
supportMail=[]
mailCount=0
conn=None
print("Support Ticket Auto Process Code is started...")
def getCurrentTime():
    now = datetime.datetime.now()
    time=now.strftime('%d.%m.%Y %H:%M')
    return time

def databaseConnect():
    global conn
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server= (#private);'
                      'Database=DWSSERVICESDB;'
                      'uid=Asanauser;'
                      'pwd=(#private);'
                      'Trusted_Connection=no')
def transDatabaseUpdate(code,department):
    databaseConnect()
    cursor=conn.cursor()
    cursor.execute('''
            UPDATE [DWSSERVICESDB].[Asana].[Asana_Process_Tracking] 
            SET   [LastProcessCode]=?
            WHERE [Department]=?
            ''',code,department)
    conn.commit()    
   
    

def transDatabase(code,departmentid):
    databaseConnect()
    cursor=conn.cursor()
    cursor.execute('''
            UPDATE [DWSSERVICESDB].[Asana].[Asana_Process_Tracking] 
            SET   [LastProcessCode]=?
            WHERE [Id]=?
            ''',code,departmentid)
    conn.commit()    
def setDatabaseLast(gid):
    task_gids=gid
    SUPPORT=[]
    
    
    for x in task_gids:
        result = client.tasks.get_task(x, opt_pretty=True,opt_fields=['custom_fields.gid','custom_fields.text_value','created_at','custom_fields.enum_value.name'])
        result['created_at']=convertTime(result['created_at'])
        try:
            if(result['custom_fields'][0]['text_value'] is not None):
                SUPPORT.append(result)
                SUPPORT=bubbleSort(SUPPORT)
            #print(SUPPORT[-1]['custom_fields'][0]['text_value'])
                if(SUPPORT[-1]['custom_fields'][0]['text_value'] is not None):
                    text_len=len(SUPPORT[-1]['custom_fields'][0]['text_value'])
                    transDatabase(removeZeroNumber(SUPPORT[-1]['custom_fields'][0]['text_value'][4:text_len]),6)
                    #print(SUPPORT[-1]['custom_fields'][0]['text_value'])
        except KeyError:
            continue      
        
def mailContent(support):
  
    if((len(support)<0) ):
       return False
 
    mesaj="Merhaba Sayın Yetkili,\n\n Eklenen Kodlar : "+ support + "\n\nToplam Çalışma süresi : " +str(round(total_execution_time,2))+" sn. \n\n Bu mail robot tarafından gönderilmiştir, lütfen dönüş yapmayınız.\n\nSaygılarımla\nIronman"

    return mesaj

def mailString(array):
  
    listToStr = ','.join([str(elem) for elem in array])
    return listToStr 
               

def getLastCode(department):
    databaseConnect()
    cursor=conn.cursor()

    cursor.execute('''
            SELECT [LastProcessCode] FROM [DWSSERVICESDB].[Asana].[Asana_Process_Tracking] 
            WHERE  [Department]=?
            ''',department)
    result=cursor.fetchone()
    #print(result[0])
    return result[0]

def getAllTasks():
    project_gids=[]
    project_gid="1191381810707817" 
    result = client.tasks.get_tasks_for_project(project_gid, opt_pretty=True)
    for x in result:
        project_gids.append(x['gid'])
    setDatabaseLast(project_gids)
    getTask(project_gids)
    

def bubbleSort(arr): 
    n = len(arr) 
    for i in range(n-1): 
        for j in range(0, n-i-1): 
            if arr[j]['created_at'] > arr[j+1]['created_at'] : 
                arr[j], arr[j+1] = arr[j+1], arr[j]
    return arr 

def convertTime(time):
    epoch = datetime.datetime.utcfromtimestamp(0)
    dt = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch =  int((dt - epoch).total_seconds())
    return epoch

def convertToNumber(number):
    if(number<100):
        return str(number).zfill(3)
    else:
        return str(number)

def removeZeroNumber(number):
    if((number[0]=='0') and (number[1]=='0')):
        return int(str(number[2:3]))
    elif(number[0]=='0'):
        return int(str(number[1:3]))
    else:
        return int(number)

def getTask(case):
    global mailCount
    task_gids=case
    CASE=[]
    temp=0
    deparment='SUPPORT'
    for x in task_gids:
        result = client.tasks.get_task(x, opt_pretty=True,opt_fields=['custom_fields.gid','custom_fields.text_value','created_at'])
        try:
         temp=result['custom_fields'][0]['text_value']
        except KeyError:
            continue
       
        if (not temp is not None):
           
            result['custom_fields'][0]['text_value']='CASE'+convertToNumber(getLastCode(deparment)+1)
            client.tasks.update_task(result['gid'],{'custom_fields':{result['custom_fields'][0]['gid']:result['custom_fields'][0]['text_value']}})
            transDatabaseUpdate(getLastCode(deparment)+1,deparment)
            CASE.append(result['custom_fields'][0]['text_value'])
            supportMail.append(result['custom_fields'][0]['text_value'])
            mailCount+=1


auth_token="1/1190298715211383:a3281b04b414973eae7feaefef6ee5dd"
client = asana.Client.access_token(auth_token)
start_time=time.time()
getAllTasks()
total_execution_time=time.time()-start_time
#print(total_execution_time)
#send to mail

mesaj=mailContent(mailString(supportMail))
if(mesaj):
    mesaj=MIMEText(mesaj)
    mail=smtplib.SMTP((private))
    mail.ehlo()
    mail.starttls()
    mesaj['From'] = 'ironman@arvatocrmturkey.com'
    mesaj['To'] = 'Dws_Team_Ext@mayen.com'
    mesaj['Subject'] = 'DWS003_Asana Auto Generated Code_SupportTickets_'+getCurrentTime()
    mail.login((private))  
    mail.sendmail("ironman@arvatocrmturkey.com","Dws_Team_Ext@mayen.com",mesaj.as_string()) 
    mailMessage=mailString(supportMail)


#databaseconnection
databaseConnect()

cursor = conn.cursor()

cursor.execute('SELECT * FROM [DWSSERVICESDB].[Asana].[Python_Logs]')

cursor.execute('''
          INSERT INTO [DWSSERVICESDB].[Asana].[Python_Logs] ([LogTypeId],[TimeStamp],[Duration],[MessageText],[Counts],[Status])
          VALUES 
          (?,GETDATE(),?,?,?,'Başarılı')
          ''',2,round(total_execution_time,2),mailMessage,mailCount)

conn.commit()
print("Support Ticket Auto Process Code is done...")

