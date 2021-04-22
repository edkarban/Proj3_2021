import sqlite3
import re
import pandas as pd
import matplotlib.pyplot as plt


# proj3_choc.py

#Student: Edward Karban
# Uniquname: edkarban@umcich.edu

# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from a database called choc.db

DBNAME = 'choc.sqlite'
DBLOC = 'choc.sqlite'
HELPLOC= 'Proj3Help.txt'
# DBLOC = 'Project 03\Proj3_2021\choc.sqlite'
# HELPLOC= 'Project 03\Proj3_2021\Proj3Help.txt'


conn = sqlite3.connect(DBLOC)
cur = conn.cursor()
cur = cur.execute('SELECT * FROM Bars')
dfbars = pd.DataFrame(cur.fetchall())
dfbars.columns = ['id','company','specific_bean_bar_name','ref','reviewdate',
'cacoapercent','companylocationid','rating','beantype','broadbeanoriginid']
cur = cur.execute('SELECT * FROM Countries')
dfcountries = pd.DataFrame(cur.fetchall())
dfcountries.columns = ['id','alpha2','alpha3','englishname','region','subregion','population','area']
conn.close()

dfco1 = dfcountries.copy()
dfco2 = dfcountries.copy()
dfco1.columns = dfco1.columns.map(lambda x: str(x) + '_sell')
dfco2.columns = dfco2.columns.map(lambda x: str(x) + '_source')

df = pd.merge(left=dfbars, right=dfco1, left_on=['companylocationid'], right_on=['id_sell'])
df = pd.merge(left=df, right=dfco2, left_on=['broadbeanoriginid'], right_on=['id_source'])


# Part 1: Implement logic to process user commands

def process_command(command):
    qry = {'param1':'bars', 'param2':None, 'param3':'sell', 'param4':'ratings', 'param5':'top', 'param6':int(10), 'param7':'country', 'param8':None}


    try:
        com = command.split(' ')
        for item in com:
            if 'bars' in com[0]:
                if 'country=' in item:
                    i = item.replace('country=','')
                    qry.update({'param2':i})
                elif 'region=' in item:
                    i = item.replace('region=','')
                    qry.update({'param2':i,'param7':'region'})
                if 'source' in item:
                    qry.update({'param3':'source'})
                if 'cocoa' in item:
                    qry.update({'param4':'cocoa'})
                if 'bottom' in item:
                    qry.update({'param5':'bottom'})
                if 'barplot' in item:
                    qry.update({'param8':'barplot'})


            elif 'companies' in com[0]:
                qry.update({'param1':'companies'})
                if 'country=' in item:
                    i = item.replace('country=','')
                    qry.update({'param2':i})
                elif 'region=' in item:
                    i = item.replace('region=','')
                    qry.update({'param2':i,'param7':'region'})
                if 'cocoa' in item:
                    qry.update({'param4':'cocoa'})
                elif 'number_of_bars' in item:
                    qry.update({'param4':'number_of_bars'})
                if 'bottom' in item:
                    qry.update({'param5':'bottom'})
                if 'barplot' in item:
                    qry.update({'param8':'barplot'})


            elif 'countries' in com[0]:
                qry.update({'param1':'countries'})
                if 'region=' in item:
                    i = item.replace('region=','')
                    qry.update({'param2':i,'param7':'region'})
                if 'source' in item:
                    qry.update({'param3':'source'})
                if 'cocoa' in item:
                    qry.update({'param4':'cocoa'})
                elif 'number_of_bars' in item:
                    qry.update({'param4':'number_of_bars'})
                if 'bottom' in item:
                    qry.update({'param5':'bottom'})
                if 'barplot' in item:
                    qry.update({'param8':'barplot'})

            elif com[0] == 'regions':
                qry.update({'param1':'regions'})
                if 'source' in item:
                    qry.update({'param3':'source'})
                if 'cocoa' in item:
                    qry.update({'param4':'cocoa'})
                elif 'number_of_bars' in item:
                    qry.update({'param4':'number_of_bars'})
                if 'bottom' in item:
                    qry.update({'param5':'bottom'})
                if 'barplot' in item:
                    qry.update({'param8':'barplot'})

            else:
                if 'country=' in com[0]:
                    i = item.replace('country=','')
                    qry.update({'param2':i})
                elif 'region=' in item:
                    i = item.replace('region=','')
                    qry.update({'param2':i,'param7':'region'})
                if 'source' in item:
                    qry.update({'param3':'source'})
                if 'cocoa' in item:
                    qry.update({'param4':'cocoa'})
                if 'bottom' in item:
                    qry.update({'param5':'bottom'})
                if 'barplot' in item:
                    qry.update({'param8':'barplot'})
    except:
        print('Please enter a valid command, or type "help" or "exit": ')

    ii = re.findall(r'\d{1,3}', command)
    if len(ii) > 0:
        ii = ii[0]
        qry.update({'param6':ii})

    ret = table_query(qry)
    return ret



def table_query(q_params):

    p1 = q_params['param1']
    p2 = q_params['param2']
    p3 = q_params['param3']
    p4 = q_params['param4']
    p5 = q_params['param5']
    p6 = q_params['param6']
    p7 = q_params['param7']
    p8 = q_params['param8']

    table = pd.DataFrame()

    if p1 == 'bars':
        table = df[['specific_bean_bar_name','company','englishname_sell','rating','cacoapercent','englishname_source','region_sell','alpha2_sell','region_source','alpha2_source']]
        if p3 == 'sell':
            if p7 == 'region' and p2 != None:
                table = table[table['region_sell'] == p2]
            elif p7 == 'country' and len(str(p2)) == 2:
                table = table[table['alpha2_sell'] == p2]
            elif p7 == 'country' and p2 != None:
                table = table[table['englishname_sell'] == p2]
        elif p3 == 'source':
            if p7 == 'region' and p2 != None:
                table = table[table['region_source'] == p2]
            elif p7 == 'country' and len(str(p2)) == 2:
                table = table[table['alpha2_source'] == p2]
            elif p7 == 'country' and p2 != None:
                table = table[table['englishname_source'] == p2]

        if p4 == 'ratings' and p5 == 'top':
            table = table.sort_values(by=['rating'], ascending=False)
        elif p4 == 'ratings' and p5 == 'bottom':
            table = table.sort_values(by=['rating'], ascending=True)
        elif p4 == 'cocoa' and p5 == 'top':
            table = table.sort_values(by=['cacoapercent'], ascending=False)
        elif p4 == 'cocoa' and p5 == 'bottom':
            table = table.sort_values(by=['cacoapercent'], ascending=True)

        table = table.drop(['region_sell','alpha2_sell','region_source','alpha2_source'], axis=1)
        table.columns = ['Specific Bean Name','Company Name','Company Location','Rating','Cocoa Percent','Broad Bean Origin']

        if p8 == 'barplot':
            table = table.iloc[0:int(p6)]
            if p4 == 'rating':
                plt.bar(table['Specific Bean Name'], table['Rating'])
                plt.xticks(rotation=35)
                plt.show
            elif p4 == 'cocoa':
                plt.bar(table['Specific Bean Name'], table['Cocoa Percent'])
                plt.xticks(rotation=35)
                plt.show()
        else:
            print(table[0:int(p6)].to_string(index=False),'\n\n')

        return(table[0:int(p6)].to_records(index=False))



    if p1 == 'companies':
        table = df[['company','englishname_sell','rating','cacoapercent','englishname_source','region_sell','alpha2_sell']]
        if p7 == 'region' and p2 != None:
            table = table[table['region_sell'] == p2]
        elif p7 == 'country' and len(str(p2)) == 2:
            table = table[table['alpha2_sell'] == p2]
        elif p7 == 'country' and p2 != None:
            table = table[table['englishname_sell'] == p2]

        table = table.groupby(['company', 'englishname_sell']).agg(['mean','count'])
        table = table.reset_index()
        table.columns = ['Company Name','Company Location','Average Bar Rating','No. Bars','Average Cacoa Percent','No. Bars2']
        table = table[table['No. Bars'] > 4]

        if p4 == 'ratings':
            table = table.drop(['No. Bars','Average Cacoa Percent','No. Bars2'], axis=1)
        elif p4 == 'cocoa':
            table = table.drop(['No. Bars','Average Bar Rating','No. Bars2'], axis=1)
        elif p4 == 'number_of_bars':
            table = table.drop(['Average Bar Rating','Average Cacoa Percent','No. Bars2'], axis=1)

        if p4 == 'ratings' and p5 == 'top':
            table = table.sort_values(by=['Average Bar Rating'], ascending=False)
        elif p4 == 'ratings' and p5 == 'bottom':
            table = table.sort_values(by=['Average Bar Rating'], ascending=True)
        elif p4 == 'cocoa' and p5 == 'top':
            table = table.sort_values(by=['Average Cacoa Percent'], ascending=False)
        elif p4 == 'cocoa' and p5 == 'bottom':
            table = table.sort_values(by=['Average Cacoa Percent'], ascending=True)
        elif p4 == 'number_of_bars' and p5 == 'top':
            table = table.sort_values(by=['No. Bars'], ascending=False)
        elif p4 == 'number_of_bars' and p5 == 'bottom':
            table = table.sort_values(by=['No. Bars'], ascending=True)

        if p8 == 'barplot':
            table = table.iloc[0:int(p6)]
            if p4 == 'rating':
                plt.bar(table['Company Name'], table['Average Bar Rating'])
                plt.xticks(rotation=35)
                plt.show
            elif p4 == 'cocoa':
                plt.bar(table['Company Name'], table['Cocoa Percent'])
                plt.xticks(rotation=35)
                plt.show()
            elif p4 == 'number_of_bars':
                plt.bar(table['Company Name'], table['No. Bars'])
                plt.xticks(rotation=35)
                plt.show()
        else:
            print(table[0:int(p6)].to_string(index=False),'\n\n')

        return(table[0:int(p6)].to_records(index=False))


    if p1 == 'countries':
        table = df[['englishname_sell','region_sell','rating','cacoapercent','englishname_source','region_source']]
        if p3 == 'sell':
            table = table.groupby(['englishname_sell', 'region_sell']).agg(['mean','count'])
        if p3 == 'source':
            table = table.groupby(['englishname_source', 'region_source']).agg(['mean','count'])

        table = table.reset_index()

        table.columns = ['Country','Region','Average Bar Rating','No. Bars','Average Cacoa Percent','No. Bars2']
        table = table[table['No. Bars'] > 4]

        if p4 == 'ratings':
            table = table.drop(['No. Bars','Average Cacoa Percent','No. Bars2'], axis=1)
        elif p4 == 'cocoa':
            table = table.drop(['No. Bars','Average Bar Rating','No. Bars2'], axis=1)
        elif p4 == 'number_of_bars':
            table = table.drop(['Average Bar Rating','Average Cacoa Percent','No. Bars2'], axis=1)

        if p4 == 'ratings' and p5 == 'top':
            table = table.sort_values(by=['Average Bar Rating'], ascending=False)
        elif p4 == 'ratings' and p5 == 'bottom':
            table = table.sort_values(by=['Average Bar Rating'], ascending=True)
        elif p4 == 'cocoa' and p5 == 'top':
            table = table.sort_values(by=['Average Cacoa Percent'], ascending=False)
        elif p4 == 'cocoa' and p5 == 'bottom':
            table = table.sort_values(by=['Average Cacoa Percent'], ascending=True)
        elif p4 == 'number_of_bars' and p5 == 'top':
            table = table.sort_values(by=['No. Bars'], ascending=False)
        elif p4 == 'number_of_bars' and p5 == 'bottom':
            table = table.sort_values(by=['No. Bars'], ascending=True)

        if p8 == 'barplot':
            table = table.iloc[0:int(p6)]
            if p4 == 'rating':
                plt.bar(table['Country'], table['Average Bar Rating'])
                plt.xticks(rotation=35)
                plt.show
            elif p4 == 'cocoa':
                plt.bar(table['Country'], table['Cocoa Percent'])
                plt.xticks(rotation=35)
                plt.show()
            elif p4 == 'number_of_bars':
                plt.bar(table['Country'], table['No. Bars'])
                plt.xticks(rotation=35)
                plt.show()
        else:
            print(table[0:int(p6)].to_string(index=False),'\n\n')

        return(table[0:int(p6)].to_records(index=False))

    if p1 == 'regions':
        table = df[['region_sell','rating','cacoapercent','region_source']]
        if p3 == 'sell':
            table = table.groupby(['region_sell']).agg(['mean','count'])
        if p3 == 'source':
            table = table.groupby(['region_source']).agg(['mean','count'])

        table = table.reset_index()
        table.columns = ['Region','Average Bar Rating','No. Bars','Average Cacoa Percent','No. Bars2']
        table = table[table['No. Bars'] > 4]

        if p4 == 'ratings':
            table = table.drop(['No. Bars','Average Cacoa Percent','No. Bars2'], axis=1)
        elif p4 == 'cocoa':
            table = table.drop(['No. Bars','Average Bar Rating','No. Bars2'], axis=1)
        elif p4 == 'number_of_bars':
            table = table.drop(['Average Bar Rating','Average Cacoa Percent','No. Bars2'], axis=1)

        if p4 == 'ratings' and p5 == 'top':
            table = table.sort_values(by=['Average Bar Rating'], ascending=False)
        elif p4 == 'ratings' and p5 == 'bottom':
            table = table.sort_values(by=['Average Bar Rating'], ascending=True)
        elif p4 == 'cocoa' and p5 == 'top':
            table = table.sort_values(by=['Average Cacoa Percent'], ascending=False)
        elif p4 == 'cocoa' and p5 == 'bottom':
            table = table.sort_values(by=['Average Cacoa Percent'], ascending=True)
        elif p4 == 'number_of_bars' and p5 == 'top':
            table = table.sort_values(by=['No. Bars'], ascending=False)
        elif p4 == 'number_of_bars' and p5 == 'bottom':
            table = table.sort_values(by=['No. Bars'], ascending=True)

        if p8 == 'barplot':
            table = table.iloc[0:int(p6)]
            if p4 == 'rating':
                plt.bar(table['Region'], table['Average Bar Rating'])
                plt.xticks(rotation=35)
                plt.show
            elif p4 == 'cocoa':
                plt.bar(table['Region'], table['Cocoa Percent'])
                plt.xticks(rotation=35)
                plt.show()
            elif p4 == 'number_of_bars':
                plt.bar(table['Region'], table['No. Bars'])
                plt.xticks(rotation=35)
                plt.show()
        else:
            print(table[0:int(p6)].to_string(index=False),'\n\n')

        return(table[0:int(p6)].to_records(index=False))



def load_help_text():
    with open(HELPLOC) as f:
        return f.read()

# Part 2 & 3: Implement interactive prompt and plotting. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command (or type "help" or "exit"): ')

        if response == 'help':
            print(help_text)
            continue

        else:
            i = process_command(response)
            continue


# Make sure nothing runs or prints out when this file is run as a module/library
if __name__=="__main__":
    interactive_prompt()