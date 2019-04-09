import gurobipy as grb
import pandas as pd
import sqlite3
from itertools import chain
from DataAnalysis import uploadData, getCleanData, getAwayDict, getHomeDict
import numpy as np
#create DB and get data from DB
uploadData()
GV,NK,OP,TM = getCleanData()

# # #define indicies, data and variabless
away = getAwayDict() #form: 'away team' [home teams played]
home = getHomeDict() #form: 'home team' [away teams played]
teams = TM.set_index('Team').T.to_dict('list')
slots = list(set(NK.iloc[:,1]))
networks = list(set(NK.iloc[:,2]))
weeks = range(1,18) # 1-17 as list
thursdayslots= ['THUN','THUE','THUL']

#make model
NFLmodel = grb.Model('NFL model')
NFLmodel.modelSense = grb.GRB.MAXIMIZE #objective function goal GRB. = constant
NFLmodel.update() #need to update after each change to model - after constraints, model, and variables

# # make variables - use dictionaries - does positivity constraint for you...
games = {} # game information
season = [] # tuple list version for cleaner constraints

for row in (GV.values.tolist()):
    a, h, w, s, n, q = row
    games[a, h, w, s, n] = NFLmodel.addVar(obj = q, vtype=grb.GRB.BINARY, name = 'GAME(%s)v(%s)slt(%s)wk(%s)on(%s)' % (h, a, s, w, n)) #if game played w this config
    season.append(tuple(row[:-1]))
season = grb.tuplelist(season)
# print(season.select("MIN",'*','*','*','*'))

#add constraints
myConstrs = {}

#01 matchups are only played one time
for t in teams:
    for h in away[t]:
        cName = '01_matchup_once-%s-%s' % (h, t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[t,h,w,s,n] for t,h,w,s,n in season.select(t,h,'*','*','*')) == 1, name=cName)
NFLmodel.update()

#02 teams play once a week
for w in range(1,18):
    for t in teams:
        cName = '02_one_game_per_week-%s-%s' % (w,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[t,h,w,s,n] for t,h,w,s,n in season.select(t,'*',w,'*','*'))+
                                              grb.quicksum(games[a,t,w,s,n] for a,t,w,s,n in season.select('*',t,w,'*','*')) == 1, name=cName)
NFLmodel.update()

#03 byes between weeks 4 and 12 - already accounted for in data but doesn't hurt to have
for w in chain(range(1,4),range(13,18)):
    cName = '03_byeweek4-12-limits-%s' % (w)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','BYE',w,'SUNB','BYE'))== 0, name=cName)
NFLmodel.update()

#04 no more than 6 byes in a given week
for w in range(4,13): #could be (1,18) but doesn't matter due to #03
    cName = '04_byes_per_week_limit-%s' % (w)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','BYE',w,'SUNB','BYE'))<= 6, name=cName)
NFLmodel.update()

#05 no team with early bye in 2017 can have bye in week 4 of 2018
byeteams = ['MIA','TB'] #internet checked.
for t in byeteams:
    cName = '05_no_early_bye_repeats-%s' % (t)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[t,h,w,s,n] for t,h,w,s,n in season.select(t,'BYE',4,'SUNB','BYE'))== 0, name=cName)
NFLmodel.update()

#06 ONE THUN for weeks 1-15
for w in range(1,18):
    if w == 16 or w == 17: #none in 16 or 17
        cName = '06_no_thursdaynights_week-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'THUN','*'))== 0, name=cName)
    else:
        cName = '06_one_thursdaynights_week-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'THUN','*'))== 1, name=cName)    
NFLmodel.update()

#07 two SATE and SATL in weeks 15 and 16
nightslots=['SATE','SATL']
for w in range(15,17):
    for time in nightslots:
        cName = '07_two_saturdaynight_wk-%s-%s' % (w, time)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,time,'*'))== 1, name=cName)
NFLmodel.update()

#08a one DH in weeks 1 tho 16 and two in week 17
for w in range(1,18):
    if w != 17:
        cName = '08a_one_DH_wk-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUND','*'))== 1, name=cName)
    if w == 17:
        cName = '08a_two_DH_wk-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUND','*'))== 2, name=cName)
NFLmodel.update()

#08b CBS and FOX not two SUND in a row
for nk in ['CBS','FOX']:
    for i in range(1,16):
        wk=[w for w in range(i,i+3)]
        cName = '08b_two_DH_row_windowstart-%s-%s' % (i,nk)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk3window,s,n] for a,h,wk3window,s,n in season.select('*','*',wk,'SUND',nk))<= 2, name=cName)
NFLmodel.update()

#08c CBS and FOX double header in week 17
for nk in ['CBS','FOX']:
    cName = '08c_DH_wk17-%s' % (nk)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',17,'SUND',nk))== 1, name=cName)
NFLmodel.update()

#09 one SUNN per week in 1-16 and none in wk 17
for w in range(1,18):
    if w != 17:
        cName = '09_one_SUNN_wk-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUNN','*'))== 1, name=cName)
    if w == 17:
        cName = '09_no_SUNN_wk-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUNN','*'))== 0, name=cName)
NFLmodel.update()

#10a two MONN in week one
cName = '10a_two_MONN_wk-%s' % (1)
myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',1,'MONN','*'))== 2, name=cName)
NFLmodel.update()

#10b One of the monday night games in week 1 is hosted by a team on the west cost or in moutain time
moutainwestteams = ['LAC','SF','SEA','OAK','LAR','DEN','ARI']
for w in [1]:
    cName = '10b_MONN_HOMETEAM_wk-%s' % (w)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*',moutainwestteams,w,'MONN','*'))>= 1, name=cName)
NFLmodel.update()

#10c one monday night game in weeks 2-16 and none in week 17
for w in range(2,18):
    if w != 17:
        cName = '10c_one_MONN_wk-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'MONN','*'))== 1, name=cName)
    if w == 17:
        cName = '10c_zero_MONN_wk-%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'MONN','*'))== 0, name=cName)
NFLmodel.update()

#11 no more than 4 home/away games in a row
for t in teams:
    for i in range(1,15):
        wk=[w for w in range(i,i+4)]
        cName = '11_four_home_row_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk4window,s,n] for a,h,wk4window,s,n in season.select('*',t,wk,'*','*'))<= 3, name=cName)
        cName = '11_four_away_row_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk4window,s,n] for a,h,wk4window,s,n in season.select('*',t,wk,'*','*'))>= 1, name=cName)
NFLmodel.update()

#12 no 3 consecutive home/away in wks 1-5, 15-17
for t in teams:
    for i in chain(range(1,4),range(15,16)):
        wk=[w for w in range(i,i+3)]
        cName = '12_three_home_row_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk4window,s,n] for a,h,wk4window,s,n in season.select('*',t,wk,'*','*'))<= 3, name=cName)
        cName = '12_three_away_row_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk4window,s,n] for a,h,wk4window,s,n in season.select(t,'*',wk,'*','*'))<= 3, name=cName)
NFLmodel.update()


#13 2 home/away every 6 weeks
for t in teams:
    for i in range(1,13):
        wk=[w for w in range(i,i+6)]
        cName = '13_2homegames_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk6window,s,n] for a,h,wk6window,s,n in season.select('*',t,wk,'*','*'))>= 2, name=cName)
        cName = '13_2awaygames_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk6window,s,n] for a,h,wk6window,s,n in season.select(t,'*',wk,'*','*'))>= 2, name=cName)
NFLmodel.update()

#14 4 home/away every 10 weeks
for t in teams:
    for i in range(1,9):
        wk=[w for w in range(i,i+10)]
        cName = '14_4homegames_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk6window,s,n] for a,h,wk6window,s,n in season.select('*',t,wk,'*','*'))>= 4, name=cName)
        cName = '14_4awaygames_windowstart-%s-%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk6window,s,n] for a,h,wk6window,s,n in season.select(t,'*',wk,'*','*'))>= 4, name=cName)
NFLmodel.update()

#15 away on thursday night are home the week before (assume first week is okay and BYES are not okay) - can only have away prev wk or thun not both
for t in teams:
    for w in range(2,18):
        cName = '15_thursnightawayprevwkhome-%s-%s' % (t,w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wprev,s,n] for a,h,wprev,s,n in season.select(t,'*',w-1,'*','*')) +\
                                              grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'*',w,thursdayslots,'*'))<=1, name=cName)
NFLmodel.update() 

#16 monday night cannot play thursday night for next two weeks
for t in teams:
    for w in range(1,16):
        cName = '16_monnight-nothurstwowks-%s-3wkwindow-%s' % (t,w)
        myConstrs[cName] = NFLmodel.addConstr(2*grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'*',w,'MONN','*'))+\
                                              2*grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*',t,w,'MONN','*'))+\
                                              grb.quicksum(games[a,h,wnxtnext,s,n] for a,h,wnxtnext,s,n in season.select('*',t,w+1,thursdayslots,'*'))+\
                                              grb.quicksum(games[a,h,wnxtnext,s,n] for a,h,wnxtnext,s,n in season.select(t,'*',w+2,thursdayslots,'*'))<=2, name=cName)
NFLmodel.update() 

#17 all thusday teams will play at home the previous week (eliminates bye and first week issue in number 15)
for t in teams:
    for w in range(2,18):
        cName = '17_thursnightprevwkhome-%s-%s' % (t,w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wprev,s,n] for a,h,wprev,s,n in season.select(t,'*',w-1,'*','*')) +\
                                              grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'*',w,thursdayslots,'*'))+\
                                              grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*',t,w,thursdayslots,'*'))<=1, name=cName)
NFLmodel.update() 

#18 team playing a bye in previous week cannot play on a thursday this week (fixes number 15 bye issue)
for t in teams:
    for w in range(2,18):
        cName = '18_prevwkbye-nothursday-%s-%s' % (t,w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'BYE',w-1,'SUNB','BYE')) +\
                                          grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'*',w,thursdayslots,'*'))+\
                                          grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*',t,w,thursdayslots,'*'))<=1, name=cName)
NFLmodel.update()                           

#19 Week 17 games can only consist of games between division opponents
cName = '19_wk17-no-non-division-games'
myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',17,'*','*')if teams[h][1]!=teams[a][1]) ==0, name=cName)
NFLmodel.update()

#20 no thrusday night away play home team greater than 1 zone away
for t in teams:
    for w in range(1,18):
        cName = '20_thursdaytimezoneforawayteamtravel-%s-%s' % (w,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'*',w,'THUN','*') \
                                              if abs(teams[t][2]-teams[h][2])>=2)==0, name=cName)
NFLmodel.update()

NFLmodel.update()
#check if proper formulation
NFLmodel.write('test.lp') 

#model solving tuning params:
# NFLmodel.setParam('MIPFocus',0)
# NFLmodel.setParam('TimeLimit',285)
# NFLmodel.setParam('SolutionLimit',1)
# NFLmodel.setParam('MIPGap',0.0001)

#solve:
# GRBRead(myModel, fullPathToMSTFile) #WARM MST PRELOAD

NFLmodel.optimize()

##Results:
#if its converged optimal:
if NFLmodel.Status == grb.GRB.OPTIMAL:
    NFLmodel.write('solution.sol') #write the solution to file
    NFLmodel.write('solution.mst') #write the solution to file

    #make DB and write output
    conn = sqlite3.connect('NFL.db')
    c = conn.cursor()
    try:
        table_name = 'Schedule'
        c.execute('''CREATE TABLE %s (
                    Away text,
                    Home text,
                    Week int,
                    Slot text,
                    Network text)''' %
                    (table_name))
    except:
        c.execute("Delete from %s" % (table_name)) #delete table data if already created


    for key, value in games.items():
        if value.x>0:
            insert = list(key)
            c.executemany('insert into %s values (?,?,?,?,?)' % (table_name), (insert,))
    conn.commit()
    #print count
    c.execute('select count(*) from %s'% (table_name))
    result = c.fetchone()
    print('Inserted ' + str(result[0])+ ' records into the '+ table_name+ ' table.')

elif NFLmodel.Status == grb.GRB.INFEASIBLE:
    NFLmodel.computeIIS()
    NFLmodel.write('myInfeasibleModel.ilp')

elif NFLmodel.Status == grb.GRB.TIME_LIMIT or grb.GRB.SUBOPTIMAL:
    print("Time limit reached...Writing non-optimal solution")
    NFLmodel.write('solution.sol')
    NFLmodel.write('solution.mst') 

else:
    print('What Happened? Not Sub optimal and Not Infesible and Not Optimal. HALP')

