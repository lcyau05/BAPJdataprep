import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import sched
import time

event_schedule = sched.scheduler(time.time, time.sleep)
scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('bapjsurvey.json', scope)
client = gspread.authorize(creds)


sheet = client.open_by_url(
    'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
    'Form responses 1')

data = sheet.get_all_values()

def timestamp():
    com = []
    for x in data[1:]:
        com.append(x[0].upper())
    return com


def companydetails():
    com = []
    for x in data[1:]:
        com.append(x[2].upper())
    return com


def EmploymentDetails():
    com = []
    for x in data[1:]:
        com.append(x[1].upper())
    return com


def Culture():
    com = []
    for x in data[1:]:
        com.append(x[4].upper())
    return com


def Happiness():
    com = []
    for x in data[1:]:
        com.append(x[3].upper())
    return com


def Buddy():
    com = []
    for x in data[1:]:
        com.append(x[5].upper())
    return com


def Buddy_Team():
    com = []
    for x in data[1:]:
        com.append(x[6].upper())
    return com


def Discrimination():
    com = []
    for x in data[1:]:
        com.append(x[7].upper())
    return com


def Discrimination_Types():
    com = []
    for x in data[1:]:
        if x[8] == '':
            com.append(x[8])
        else:
            y = x[8].split(',')
            com.append(y)
    return com


def AddressIssue():
    com = []
    for x in data[1:]:
        if x[9] == '':
            com.append(x[9])
        else:
            y = x[9].split(',')
            com.append(y)
    return com


def resign():
    com = []
    for x in data[1:]:
        com.append(x[10].upper())
    return com


def conducive():
    com = []
    for x in data[1:]:
        if x[11] == '':
            com.append(x[11])
        else:
            y = x[11].split(',')
            com.append(y)
    return com


def loyal():
    com = []
    for x in data[1:]:
        com.append(x[12].upper())
    return com


def stay():
    com = []
    for x in data[1:]:
        if x[13] == '':
            com.append(x[13])
        else:
            y = x[13].split(',')
            com.append(y)
    return com


def refresh(sc):
    df = pd.DataFrame({'TimeStamp': timestamp(), 'EmploymentDetails': EmploymentDetails(), 'Company': companydetails(),
                       'Happy There?': Happiness(), 'Culture Type': Culture(), 'Buddy There?': Buddy(),
                       'Having a BUDDY makes me more productive?': Buddy_Team(),
                       'Are there any Discrimination Cases in your workplace?': Discrimination(),
                       'Types of Discriminative acts witnessed or experienced': Discrimination_Types(),
                       'How can your company address this discriminative issue?': AddressIssue(),
                       'Likely to resign if the issue persists?': resign(),
                       'What makes your company a conducive workplace environment?': conducive(),
                       'Would you likely to remain loyal and stay in the company?': loyal(),
                       'What are the factors as to remain regardless of your happiness at your workplace?': stay()})

    clean1 = pd.DataFrame({'TimeStamp': timestamp(), 'EmploymentDetails': EmploymentDetails(), 'Company': companydetails(),
                       'Happy There?': Happiness(), 'Culture Type': Culture(), 'Buddy There?': Buddy(),
                       'Having a BUDDY makes me more productive?': Buddy_Team(),
                       'Are there any Discrimination Cases in your workplace?': Discrimination()})

    clean2 = pd.DataFrame(
        {'TimeStamp': timestamp(),
         'Types of Discriminative acts witnessed or experienced': Discrimination_Types()})

    clean3 = pd.DataFrame(
        {'TimeStamp': timestamp(),
         'How can your company address this discriminative issue?': AddressIssue()})

    clean4 = pd.DataFrame(
        {'TimeStamp': timestamp(),
         'Likely to resign if the issue persists?': resign()})

    clean5 = pd.DataFrame(
        {'TimeStamp': timestamp(),
         'What makes your company a conducive workplace environment?': conducive()})

    clean6 = pd.DataFrame(
        {'TimeStamp': timestamp(),
         'Would you likely to remain loyal and stay in the company?': loyal()})

    clean7 = pd.DataFrame(
        {'TimeStamp': timestamp(),
         'What are the factors as to remain regardless of your happiness at your workplace?': stay()})


    newdf = df.explode('Types of Discriminative acts witnessed or experienced')
    newdf2 = newdf.explode('How can your company address this discriminative issue?')
    newdf3 = newdf2.explode('What makes your company a conducive workplace environment?')
    newdf4 = newdf3.explode('What are the factors as to remain regardless of your happiness at your workplace?')
    print(newdf4)

    clean22 = clean2.explode('Types of Discriminative acts witnessed or experienced')
    clean33 = clean3.explode('How can your company address this discriminative issue?')
    clean55 = clean5.explode('What makes your company a conducive workplace environment?')
    clean77 = clean7.explode('What are the factors as to remain regardless of your happiness at your workplace?')

    sheet = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'CleanedResponse')

    sheet1 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet1')

    sheet2 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet2')

    sheet3 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet3')

    sheet4 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet4')

    sheet5 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet5')

    sheet6 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet6')

    sheet7 = client.open_by_url(
        'https://docs.google.com/spreadsheets/d/1KD4ccPCLM6f6Sa7vQebH_wRQSTpT8JCkzKNpSCnMDoo/edit?resourcekey#gid=314601472').worksheet(
        'Sheet7')

    sheet.clear()
    sheet1.clear()
    sheet2.clear()
    sheet3.clear()
    sheet4.clear()
    sheet5.clear()
    sheet6.clear()
    sheet7.clear()

    set_with_dataframe(sheet, newdf4)
    set_with_dataframe(sheet1, clean1)
    set_with_dataframe(sheet2, clean22)
    set_with_dataframe(sheet3, clean33)
    set_with_dataframe(sheet4, clean4)
    set_with_dataframe(sheet5, clean55)
    set_with_dataframe(sheet6, clean6)
    set_with_dataframe(sheet7, clean77)
    event_schedule.enter(30, 1, refresh, (sc,))


event_schedule.enter(30, 1, refresh, (event_schedule,))
event_schedule.run()
