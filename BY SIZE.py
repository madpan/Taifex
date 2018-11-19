import pandas as pd
from os import listdir
from os.path import join

statpath = 'C:/Users/user/Desktop/taifex-parser-master/futuresData'
statfiles = listdir(statpath)


for f in statfiles:
    statfullpath= join(statpath,f)
    practice=pd.read_csv(statfullpath,encoding='cp950',sep=",",header=0)
    corp=practice.groupby('期貨商名稱')
    date=practice['交易日期'].loc[1]
    size=practice.groupby('契約名稱').size()
    size=pd.DataFrame(size)
    size.columns=['總交易次數']
    for i in corp:
        firm=i[0]
        a=i[1]
        b=a.groupby('契約名稱').size()
        temp=pd.DataFrame(b)
        temp.columns=[firm]
        print(firm)
        print(practice['交易日期'].loc[1])
        print('\n')            
        size=size.join(temp)
        size['交易日期']=str(date)
        index=size.columns
    size.to_csv('C:/Users/user/Desktop/taifex-parser-master/size.csv',encoding="cp950",index = True ,header = 0 ,mode= 'a+' )
    
final=pd.read_csv('C:/Users/user/Desktop/taifex-parser-master/size.csv',encoding="cp950" , header = None , error_bad_lines=False ,index_col= 0  )
final.columns=index 
final=final     
final.fillna(0)
final.to_csv('C:/Users/user/Desktop/taifex-parser-master/final.csv',encoding="cp950",index = True  ,mode= 'a+')    
        
    
    
    
        
   

    
    
    
    
    
        
    
    










    

  
    










 




