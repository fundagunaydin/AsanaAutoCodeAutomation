import json
import asana
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import time
import datetime
import pyodbc


#Mail string implementation
rpaMail=[]
dwsMail=[]
dwhMail=[]
mailCount=0
conn=None

def getCurrentTime():
    now = datetime.datetime.now()
    time=now.strftime('%d.%m.%Y %H:%M')
    return time

def databaseConnect():
    global conn
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=10.94.92.165\Arvatodb;'
                      'Database=DWSSERVICESDB;'
                      'uid=Asanauser;'
                      'pwd=XsUuP?$vc8aX;'
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

def getLastCode(department):
    databaseConnect()
    cursor=conn.cursor()

    cursor.execute('''
            SELECT [LastProcessCode] FROM [DWSSERVICESDB].[Asana].[Asana_Process_Tracking] 
            WHERE  [Department]=?
            ''',department)
    result=cursor.fetchone()
    return result[0]


def bubbleSort(arr): 
    n = len(arr) 
    for i in range(n-1): 
        for j in range(0, n-i-1): 
            if int(arr[j]['created_at']) > int(arr[j+1]['created_at']) : 
                arr[j], arr[j+1] = arr[j+1], arr[j]
    return arr 

def convertTime(time):
    epoch = datetime.datetime.utcfromtimestamp(0)
    dt = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
    epoch =  int((dt - epoch).total_seconds())
    return epoch

def setDatabaseLast(gid):
    task_gids=gid
    RPA=[]
    DWS=[]
    DWH=[]
    
    for x in task_gids:
        
        result = client.tasks.get_task(x, opt_pretty=True,opt_fields=['custom_fields.gid','custom_fields.text_value','created_at','custom_fields.enum_value.name'])
        #print(result['custom_fields'][0]['text_value'])
        #exit()
        #print(result)
        
        try:
            if((getRpaName(result['custom_fields'][7]['enum_value']['name'])==1) and (result['custom_fields'][0]['text_value'] is not None)):
              
                result['created_at']=convertTime(result['created_at'])
                DWH.append(result)
                DWH=bubbleSort(DWH)
                text_len=len(DWH[-1]['custom_fields'][0]['text_value'])
                transDatabase(removeZeroNumber(DWH[-1]['custom_fields'][0]['text_value'][3:text_len]),5)
                                  
            elif((getRpaName(result['custom_fields'][7]['enum_value']['name'])==2) and (result['custom_fields'][0]['text_value'] is not None)):
                result['created_at']=convertTime(result['created_at'])
                DWS.append(result)
                DWS=bubbleSort(DWS)
                text_len=len(DWS[-1]['custom_fields'][0]['text_value'])
                transDatabase(removeZeroNumber(DWS[-1]['custom_fields'][0]['text_value'][3:text_len]),4)
         
            elif((getRpaName(result['custom_fields'][7]['enum_value']['name'])==3) and (result['custom_fields'][0]['text_value'] is not None)):
                result['created_at']=convertTime(result['created_at'])
                RPA.append(result)
                RPA=bubbleSort(RPA)
                text_len=len(RPA[-1]['custom_fields'][0]['text_value'])
                transDatabase(removeZeroNumber(RPA[-1]['custom_fields'][0]['text_value'][3:text_len]),3)
        except TypeError:
            continue
      

    ##Database last code update               

def mailContent(rpa,dws,dwh):
    if(len(rpa)==0):
        rpa="-"

    if(len(dws)==0):
        dws="-"

    if(len(dwh)==0):
        dwh="-"

    if((len(rpa)<0) and (len(dws)<0) and (len(dwh)<0) ):
       return False
 
    mesaj="Merhaba Sayın Yetkili,\n\n Toplam eklenen Rpa Kodu : "+ rpa + "\n " "Toplam eklenen Dws Kodu:"  + dws + "\n " "Toplam eklenen Dwh Kodu: " +dwh+ "  \n\nToplam Çalışma süresi : " +str(round(total_execution_time,2))+" sn. \n\n Bu mail robot tarafından gönderilmiştir, lütfen dönüş yapmayınız.\n\nSaygılarımla\nIronman"

    return mesaj


def mailString(array):
  
    listToStr = ','.join([str(elem) for elem in array])
    return listToStr 


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

def getAllTasks():
  
    project_gids=[]
    project_gid="1171667575413590" 
    result = client.tasks.get_tasks_for_project(project_gid, opt_pretty=True)
    for x in result:
        project_gids.append(x['gid'])
    setDatabaseLast(project_gids)
    getTask(project_gids)

def getRpaName(name):

    if(name=="İş Zekası ve Raporlama"):
        return 1
    elif(name=="Dijital İş Gücü Çözümleri"):
        return 2
    else:
        return 3

def processCode(department):
    rpaText='RPA'
    dwsText='DWS'
    dwhText='DWH'
    
    if(department=="İş Zekası ve Raporlama"):
      
        return dwhText
    elif(department=='Dijital İş Gücü Çözümleri'):
        return dwsText
    else:
        return rpaText


def getTask(rpa):
    global mailCount
    task_gids=rpa
    RPA=[]
    DWS=[]
    DWH=[]
    activeArray=0
    activeArrayMail=[]
    temp=0
    for x in task_gids:
        result = client.tasks.get_task(x, opt_pretty=True,opt_fields=['custom_fields.gid','custom_fields.text_value','created_at','custom_fields.enum_value.name'])
        try: 
        
            if(getRpaName(result['custom_fields'][7]['enum_value']['name'])==1):
                activeArrayMail=dwhMail

            elif(getRpaName(result['custom_fields'][7]['enum_value']['name'])==2):
                activeArrayMail=dwsMail

            else:
                activeArrayMail=rpaMail
        except TypeError:
            continue
       
        try:
         temp=result['custom_fields'][0]['text_value']
         if (not temp is not None) and (result['custom_fields'][7]['enum_value']['name']!='null'):

                department=processCode(result['custom_fields'][7]['enum_value']['name'])
                result['custom_fields'][0]['text_value']=department+convertToNumber(getLastCode(department)+1)
                print("güncellemeye girdi")
                client.tasks.update_task(result['gid'],{'custom_fields':{result['custom_fields'][0]['gid']:result['custom_fields'][0]['text_value']}})
                transDatabaseUpdate(getLastCode(department)+1,department)
                print("database güncelledi")
                activeArrayMail.append(result['custom_fields'][0]['text_value'])
                mailCount+=1

        except KeyError:
            continue
        
        
      #  if(getRpaName(result['custom_fields'][8]['enum_value']['name'])==1):
       #     DWH.append(result)
           
        #elif(getRpaName(result['custom_fields'][8]['enum_value']['name'])==2):
         #   DWS.append(result)
        #else:
         #   RPA.append(result)

#Credentials
auth_token="1/1190298715211383:a3281b04b414973eae7feaefef6ee5dd"
client = asana.Client.access_token(auth_token)


start_time=time.time()
getAllTasks()
total_execution_time=time.time()-start_time
#print(total_execution_time)
#send to mail
#exit() code test flag

mesaj=mailContent(mailString(rpaMail),mailString(dwsMail),mailString(dwhMail))
if(mesaj):
    mesaj=MIMEText(mesaj)
    mail=smtplib.SMTP('smtp.arvatocrmturkey.com', 25)
    mail.ehlo()
    mail.starttls()
    mesaj['From'] = 'ironman@arvatocrmturkey.com'
    mesaj['To'] = 'Dws_Team_Ext@mayen.com'
    mesaj['Subject'] = 'DWS003_Asana Auto Generated Code_Pipeline_'+getCurrentTime()
    mail.login("ironman@arvatocrmturkey.com","Arvato5050!")  
    mail.sendmail("ironman@arvatocrmturkey.com","Dws_Team_Ext@mayen.com",mesaj.as_string()) 
    mailMessage=mailString(rpaMail)+','+mailString(dwsMail)+','+mailString(dwhMail)


#databaseconnection
databaseConnect()

cursor = conn.cursor()

cursor.execute('SELECT * FROM [DWSSERVICESDB].[Asana].[Python_Logs]')

cursor.execute('''
          INSERT INTO [DWSSERVICESDB].[Asana].[Python_Logs] ([LogTypeId],[TimeStamp],[Duration],[MessageText],[Counts],[Status])
          VALUES 
          (?,GETDATE(),?,?,?,'Başarılı')
          ''',1,round(total_execution_time,2),mailMessage,mailCount)

conn.commit()

