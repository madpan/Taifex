import rfc6266
import requests
import shutil
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import captchaSolver
from os import listdir
from os.path import join



class TWOptionParser():
    
    def __init__(self, solver):
        print('Parse TW Option')
        self.directory = './twoption/'
        self.TargetURL = 'http://www.taifex.com.tw/cht/3/dailyOptions'
        self.DownURL = 'http://www.taifex.com.tw/cht/3/dailyOptionsDown'
        self.Captcha = ''
        self.QueryDate = ''
        self.QueryDateAh = ''
        self.solver = solver
        self.createFolder()

    def createFolder(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def auto(self):
        start_time = time.time()
        self.getSession()
        self.getQueryDate()
        self.getMarketCode()
        print('\tDone!')
        print("Elapsed time:", time.time() - start_time, "seconds")

    def getSession(self):
        self.session = requests.session()
        self.header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-TW,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'www.taifex.com.tw',
            'Referer': 'http://www.taifex.com.tw/cht/3/dailyOptions',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'
        }
        self.getCaptcha()

    def getQueryDate(self):
        res = self.session.get(self.TargetURL, headers=self.header)

        if res.status_code != requests.codes.ok:
            raise Exception("Get Query Data Failed")

        soup = BeautifulSoup(res.text, features="html.parser")
        self.QueryDate = soup.find(id="queryDate").get('value')
        self.QueryDateAh = soup.find(id="queryDateAh").get('value')

    def getMarketCode(self):
        res = self.session.get(self.TargetURL, headers=self.header)

        if res.status_code != requests.codes.ok:
            raise Exception("Get Market Code Failed")

        soup = BeautifulSoup(res.text, features="html.parser")
        marketCode = [str(x.text) for x in soup.find(id="MarketCode").find_all('option')]
        
        for index, code in enumerate(marketCode):
            if index != 0:
                self.getCommodityList(index - 1)

    def getCommodityList(self, marketCode):
        payload = {
            'queryDate': str(self.QueryDate if marketCode == 0 else self.QueryDateAh),
            'marketcode': 0
        }

        res = self.session.get('http://www.taifex.com.tw/cht/3/getFcmOptcontract.do', params=payload, headers=self.header)
        
        if res.status_code != requests.codes.ok:
            raise Exception("Get Commodity List Failed")


        for com in res.json()['commodityList']:
            self.getSettleMonth(marketCode, com['FDAILYR_KIND_ID'], '')

        for com in res.json()['commodity2List']:
            self.getSettleMonth(marketCode, 'STO', com['FDAILYR_KIND_ID'])

    def getSettleMonth(self, marketCode, commodity, commodity2):
        payload = {
            'queryDate': str(self.QueryDate if marketCode == 0 else self.QueryDateAh),
            'marketcode': 0,
            'commodityId': commodity2 if commodity == 'STO' else commodity
        }

        res = self.session.get('http://www.taifex.com.tw/cht/3/getFcmOptSetMonth.do', params=payload, headers=self.header)
        
        if res.status_code != requests.codes.ok:
            raise Exception("Get Settle Month Failed")

        for setMon in res.json()['setMonList']:
            self.getType(marketCode, commodity, commodity2, setMon['FDAILYR_SETTLE_MONTH'])


    def getType(self, marketCode, commodity, commodity2, setMon):
        payload = {
            'queryDate': str(self.QueryDate if marketCode == 0 else self.QueryDateAh),
            'marketcode': 0,
            'commodityId': commodity2 if commodity == 'STO' else commodity,
            'settlemon': str(setMon)
        }
        res = self.session.get('http://www.taifex.com.tw/cht/3/getFcmOptionsType.do', params=payload, headers=self.header)
        
        if res.status_code != requests.codes.ok:
            raise Exception("Get Type Failed")
        
        for typeId in res.json()['typeList']:            
            self.postDailyOption(marketCode, commodity, commodity2, setMon, typeId['FDAILYR_PC_CODE'])
            self.postDownloadCsv(marketCode, commodity, commodity2, setMon, typeId['FDAILYR_PC_CODE'])

    def getCaptcha(self):
        res = self.session.get('http://www.taifex.com.tw/cht/captcha', stream=True, headers=self.header)

        if res.status_code != requests.codes.ok:
            raise Exception("Get Captcha Failed")
        
        with open(self.directory + 'Captcha.jpg', 'wb') as out_file:
            res.raw.decode_content = True
            shutil.copyfileobj(res.raw, out_file)

        self.cookies = res.cookies
        self.Captcha = self.resolveCaptcha(self.directory + 'Captcha.jpg')
        #img = cv2.imread('Captcha.jpg')
        #cv2.imshow('image', img)
        print('Captcha: ', self.Captcha)    
        #os.remove('Captcha.jpg')

    def resolveCaptcha(self, imagePathStr):
        return self.solver.solve(imagePathStr)

    def postDailyOption(self, marketCode, commodity, commodity2, setMon, pcCode):
        payload = {
            'captcha': str(self.Captcha),
            'commodity_id2t': str(commodity2),
            'commodity_idt': str(commodity),
            'commodityId': str(commodity),
            'commodityId2': str(commodity2),
            'curpage': '',
            'doQuery': '1',
            'doQueryPage': '',
            'marketcode': str(marketCode),
            'MarketCode': str(marketCode),
            'pccode': str(pcCode),
            'queryDate': str(self.QueryDate),
            'queryDateAh': str(self.QueryDateAh),
            'settlemon': str(setMon),
            'totalpage': ''
        }

        res = self.session.post(self.TargetURL, data=payload, headers=self.header, cookies=self.cookies)
        
        if res.status_code != requests.codes.ok:
            raise Exception("Post Option Failed")

    def postDownloadCsv(self, marketCode, commodity, commodity2, setMon, pcCode):
        print('.', end='')
        payload = {
            'captcha': '',
           'commodity_id2t': str(commodity2),
            'commodity_idt': str(commodity),
            'commodityId': str(commodity),
            'commodityId2': str(commodity2),
            'curpage': '1',
            'doQuery': '1',
            'doQueryPage': '',
            'marketcode': str(marketCode),
            'MarketCode': str(marketCode),
            'pccode': str(pcCode),
            'queryDate': str(self.QueryDate),
            'queryDateAh': str(self.QueryDateAh),
            'settlemon': str(setMon),
            'totalpage': ''
        }

        res = self.session.post(self.DownURL, data=payload, headers=self.header, cookies=self.cookies)
        
        if res.status_code != requests.codes.ok:
            raise Exception("Post Download Failed")

        if res.headers.get('Content-Disposition') == None:
            print('Download Failed', marketCode, commodity, commodity2, setMon, pcCode)
            return

        fileName = rfc6266.parse_requests_response(res).filename_unsafe

        with open(self.directory + fileName, 'wb') as fd:
            for chunk in res.iter_content(256):
                fd.write(chunk)

    def printBreakLine(self):
        print('=================================')
        
def main():
    parser = TWOptionParser(captchaSolver.CaptchaSolver('Captcha_model.hdf5'))
    parser.auto()

if __name__ == '__main__':
    main()

os.remove('C:/Users/user/Desktop/taifex-parser-master/twoption/Captcha.jpg')
path = 'C:/Users/user/Desktop/taifex-parser-master/twoption'
files = listdir(path)
fout = open("YAYA.csv","a+")
SaveFile_name=r'YAYA.csv'


for f in files:

    fullpath = join(path,f)
    option = pd.read_csv(fullpath,encoding="cp950",sep="," , header=1)
    information = pd.read_csv(fullpath,encoding="cp950",header=None,error_bad_lines=False)  
    newcol=information[0].str.split(expand=True)
    Date=newcol[0].str.split(':',expand=True).T
    Code=newcol[1].str.split(':',expand=True).T
    Name=newcol[2].str.split(':',expand=True).T
    Maturity=newcol[3].str.split(':',expand=True).T
    Callput=newcol[4].str.split(':',expand=True).T
    option['交易日期']=Date.loc[1,0]
    option['契約代號']=Code.loc[1,0]
    option['契約名稱']=Name.loc[1,0]
    option['到期月份']=Maturity.loc[1,0]
    option['買/賣權']=Callput.loc[1,0]
    
    
    if files[0] == f :        
       option.to_csv(path+'\\'+SaveFile_name,encoding="cp950", index = False , mode='a+')
       os.remove(fullpath)
    else :
       option.to_csv(path+'\\'+SaveFile_name,encoding="cp950", index = False , header= False,mode='a+')
       os.remove(fullpath)
       
option= pd.read_csv(path+'\\'+SaveFile_name,encoding='cp950',sep=',')
group = option.index[option.成交價格.notnull()].values
Revise = pd.DataFrame(columns=['期貨商代號', '期貨商名稱', '交易日期', '契約代號','契約名稱','到期月份','履約價格','成交價格','買/賣權','買進/賣出'])
for i in range(0,len(group)-1):
    subgroup = option.loc[group[i]:group[i+1]-1]
    for j in range(group[i],group[i+1]):
        if subgroup.isnull()['買進期貨商名稱'][j] == False:
                tempbuy={'期貨商代號':[subgroup['買進期貨商代號'][j]],
                         '期貨商名稱':[subgroup['買進期貨商名稱'][j]],
                         '交易日期':[subgroup['交易日期'][j]],
                         '契約代號':[subgroup['契約代號'][j]],
                         '契約名稱':[subgroup['契約名稱'][j]],
                         '到期月份':[subgroup['到期月份'][j]],
                         '履約價格':[subgroup['履約價格'][group[i]]],
                         '成交價格':[subgroup['成交價格'][group[i]]],
                         '買/賣權':[subgroup['買/賣權'][j]],
                         '買進/賣出':['買進']}
                temp=pd.DataFrame(tempbuy)
                frames = [Revise,temp]
                Revise=pd.concat(frames)
    for k in range(group[i],group[i+1]):
        if subgroup.isnull()['賣出期貨商代號'][k] == False:
                tempsell={'期貨商代號':[subgroup['賣出期貨商代號'][k]],
                          '期貨商名稱':[subgroup['賣出期貨商名稱 '][k]],
                          '交易日期':[subgroup['交易日期'][k]],
                          '契約代號':[subgroup['契約代號'][k]],
                          '契約名稱':[subgroup['契約名稱'][k]],
                          '到期月份':[subgroup['到期月份'][k]],
                          '履約價格':[subgroup['履約價格'][group[i]]],
                          '成交價格':[subgroup['成交價格'][group[i]]],
                          '買/賣權':[subgroup['買/賣權'][k]],
                          '買進/賣出':['賣出']}
                temp=pd.DataFrame(tempsell)
                frames = [Revise,temp]
                Revise=pd.concat(frames)  
                
for h in range(group[len(group)-1],len(option)):
    subgroup = option.loc[group[len(group)-1]:len(option)-1]
    if subgroup.isnull()['買進期貨商名稱'][h] == False:
        tempbuy={'期貨商代號':[subgroup['買進期貨商代號'][h]],
                 '期貨商名稱':[subgroup['買進期貨商名稱'][h]],
                 '交易日期':[subgroup['交易日期'][h]],
                 '契約代號':[subgroup['契約代號'][h]],
                 '契約名稱':[subgroup['契約名稱'][h]],
                 '到期月份':[subgroup['到期月份'][h]],
                 '履約價格':[subgroup['履約價格'][group[len(group)-1]]],
                 '成交價格':[subgroup['成交價格'][group[len(group)-1]]],
                 '買/賣權':[subgroup['買/賣權'][h]],
                 '買進/賣出':['買進']}
        temp=pd.DataFrame(tempbuy)
        frames = [Revise,temp]
        Revise=pd.concat(frames)
    if subgroup.isnull()['賣出期貨商名稱 '][h] == False:
        tempsell={'期貨商代號':[subgroup['賣出期貨商代號'][h]],
                  '期貨商名稱':[subgroup['賣出期貨商名稱 '][h]],
                  '交易日期':[subgroup['交易日期'][h]],
                  '契約代號':[subgroup['契約代號'][h]],
                  '契約名稱':[subgroup['契約名稱'][h]],
                  '到期月份':[subgroup['到期月份'][h]],
                  '履約價格':[subgroup['履約價格'][group[len(group)-1]]],
                  '成交價格':[subgroup['成交價格'][group[len(group)-1]]],
                  '買/賣權':[subgroup['買/賣權'][h]],
                  '買進/賣出':['賣出']}
        temp=pd.DataFrame(tempsell)
        frames = [Revise,temp]
        Revise=pd.concat(frames)
os.remove('C:/Users/user/Desktop/taifex-parser-master/twoption/YAYA.csv')
date='option'+Date.loc[1]+'.csv'
Final_Name = date[0]     
Revise.to_csv('C:/Users/user/Desktop/taifex-parser-master/optionData'+'\\'+Final_Name,encoding="cp950",index = False)
    
    
important=pd.DataFrame(columns=['日期','期貨商名稱','標的','是否交易','次數','買進','賣出'])
corp=Revise.groupby('期貨商名稱')
date=option['交易日期'].loc[1]
size=Revise.groupby('契約名稱').size()
size=pd.DataFrame(size)
size.columns=['交易次數']
times = Revise.groupby(['期貨商名稱','買進/賣出','契約名稱']).size()
times=pd.DataFrame(times)

for i in corp:
    firm=i[0]
    a=i[1]
    b=a.groupby('契約名稱').size()
    temptn=pd.DataFrame(b)
    temptn.columns=[firm]
    print(firm)
    print(option['交易日期'].loc[1])
    print('\n')            
    size=size.join(temptn)
    firm=size.columns
    target=size.index
    count=Revise.drop(columns=['到期月份','成交價格'])
for m in target:
    for l in firm.drop('交易次數'):
        if pd.isnull(size[l][m]) == False:
            BUY=times.loc[l,'買進',m][0] if(l,'買進',m) in times.index else 0
            SELL=times.loc[l,'賣出',m][0] if(l,'賣出',m) in times.index else 0
            TON=pd.DataFrame({'日期':str(option['交易日期'].loc[1]),
                              '期貨商名稱':l,'標的':m,'是否交易':'是',
                              '次數':size[l][m],'買進':BUY,'賣出':SELL},index=[0])
            temptn=pd.DataFrame(TON)
            frames = [important,TON]  
            important=pd.concat(frames)                    
        else:
            TONO = pd.DataFrame({'日期':str(option['交易日期'].loc[1]),
                                 '期貨商名稱':l,'標的':m,'是否交易':'否',
                                 '次數':'0','買進':'0','賣出':'0'},index=[0])
            temptn=pd.DataFrame(TONO)
            frames = [important,TONO]   
            important=pd.concat(frames)               
important.to_csv('C:/Users/user/Desktop/taifex-parser-master/TradeOrNotiption.csv',encoding="cp950",index = False,mode='a+', header= False)