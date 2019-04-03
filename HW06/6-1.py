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
        cName = '01_matchup_once_%s_%s' % (h, t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[t,h,w,s,n] for t,h,w,s,n in season.select(t,h,'*','*','*')) == 1, name=cName)
NFLmodel.update()

#02 teams play once a week
for w in range(1,18):
    for t in teams:
        cName = '02_one_game_per_week_%s_%s' % (w,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[t,h,w,s,n] for t,h,w,s,n in season.select(t,'*',w,'*','*'))+
                                              grb.quicksum(games[a,t,w,s,n] for a,t,w,s,n in season.select('*',t,w,'*','*')) == 1, name=cName)
NFLmodel.update()

#03 byes between weeks 4 and 12 - already accounted for in data but doesn't hurt to have
for w in chain(range(1,4),range(13,18)):
    cName = '03_byeweek4-12_limits_%s' % (w)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','BYE',w,'SUNB','BYE'))== 0, name=cName)
NFLmodel.update()

#04 no more than 6 byes in a given week
for w in range(4,13): #could be (1,18) but doesn't matter due to #03
    cName = '04_byes_per_week_limit_%s' % (w)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','BYE',w,'SUNB','BYE'))<= 6, name=cName)
NFLmodel.update()

#05 no team with early bye in 2017 can have bye in week 4 of 2018
byeteams = ['MIA','TB'] #internet checked.
for t in byeteams:
    cName = '05_no_early_bye_repeats_%s' % (t)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[t,h,w,s,n] for t,h,w,s,n in season.select(t,'BYE',4,'SUNB','BYE'))== 0, name=cName)
NFLmodel.update()

#06 ONE THUN for weeks 1-15
for w in range(1,18):
    if w == 16 or w == 17: #none in 16 or 17
        cName = '06_no_thursdaynights_week_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'THUN','*'))== 0, name=cName)
    else:
        cName = '06_one_thursdaynights_week_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'THUN','*'))== 1, name=cName)    
NFLmodel.update()

#07 two SATE and SATL in weeks 15 and 16
nightslots=['SATE','SATL']
for w in range(15,17):
    for time in nightslots:
        cName = '07_two_saturdaynight_wk_%s_%s' % (w, time)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,time,'*'))== 1, name=cName)
NFLmodel.update()

#08a one DH in weeks 1 tho 16 and two in week 17
for w in range(1,18):
    if w != 17:
        cName = '08a_one_DH_wk_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUND','*'))== 1, name=cName)
    if w == 17:
        cName = '08a_two_DH_wk_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUND','*'))== 2, name=cName)
NFLmodel.update()

#08b CBS and FOX not two SUND in a row
for nk in ['CBS','FOX']:
    for i in range(1,16):
        wk=[w for w in range(i,i+3)]
        cName = '08b_two_DH_row_windowstart_%s_%s' % (i,nk)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk3window,s,n] for a,h,wk3window,s,n in season.select('*','*',wk,'SUND',nk))<= 2, name=cName)
NFLmodel.update()

#08c CBS and FOX double header in week 17
for nk in ['CBS','FOX']:
    cName = '08c_DH_wk17_%s' % (nk)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',17,'SUND',nk))== 1, name=cName)
NFLmodel.update()

#09 one SUNN per week in 1-16 and none in wk 17
for w in range(1,18):
    if w != 17:
        cName = '09_one_SUNN_wk_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUNN','*'))== 1, name=cName)
    if w == 17:
        cName = '09_no_SUNN_wk_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'SUNN','*'))== 0, name=cName)
NFLmodel.update()

#10a two MONN in week one
cName = '10a_two_MONN_wk_%s' % (1)
myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',1,'MONN','*'))== 2, name=cName)
NFLmodel.update()

#10b One of the monday night games in week 1 is hosted by a team on the west cost or in moutain time
moutainwestteams = ['LAC','SF','SEA','OAK','LAR','DEN','ARI']
for w in [1]:
    cName = '10b_MONN_HOMETEAM_wk_%s' % (w)
    myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*',moutainwestteams,w,'MONN','*'))>= 1, name=cName)
NFLmodel.update()

#10c one monday night game in weeks 2-16 and none in week 17
for w in range(2,18):
    if w != 17:
        cName = '10c_one_MONN_wk_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'MONN','*'))== 1, name=cName)
    if w == 17:
        cName = '10c_zero_MONN_wk_%s' % (w)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select('*','*',w,'MONN','*'))== 0, name=cName)
NFLmodel.update()

####TODO
#11 no more than 4 home games in a row
for t in teams:
    for i in range(1,15):
        wk=[w for w in range(i,i+4)]
        cName = '11_four_home_row_windowstart_%s_%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk4window,s,n] for a,h,wk4window,s,n in season.select('*',t,wk,'*','*'))<= 3, name=cName)
        cName = '11_four_away_row_windowstart_%s_%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk4window,s,n] for a,h,wk4window,s,n in season.select('*',t,wk,'*','*'))>= 1, name=cName)
NFLmodel.update()

#no thrusday night time zone greater than 1 difference
for t in teams:
    for w in range(1,18):
        cName = '12_thursdaytimezone_%s_%s' % (w,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,w,s,n] for a,h,w,s,n in season.select(t,'*',w,'THUN','*') if abs(teams[t][2]-teams[h][2])>=2) ==0, name=cName)

#2 home in 6 and 2 away in 6
for t in teams:
    for i in range(1,13):
        wk=[w for w in range(i,i+6)]
        cName = '13_homegames_windowstart_%s_%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk6window,s,n] for a,h,wk6window,s,n in season.select('*',t,wk,'*','*'))>= 2, name=cName)
        cName = '13_awaygames_windowstart_%s_%s' % (i,t)
        myConstrs[cName] = NFLmodel.addConstr(grb.quicksum(games[a,h,wk6window,s,n] for a,h,wk6window,s,n in season.select(t,'*',wk,'*','*'))>= 2, name=cName)
NFLmodel.update()

# thursdayslots= ['THUN','THUE','THUL']
# for t in teams:
#     for w in range(1,16):
#     cName = '14_four_away_row_windowstart_%s_%s' % (i,t)
#     myConstrs[cName] = NFLmodel.addConstr(games[t,'BYE',w-1,'SUNB','BYE'] + grb.quicksum( games[t,h,w,s,n] for t,h,w,s,n in season.select('t','*',w,thursdayslots,'*')+games[a,t,w,s,n] for a,t,w,s,n in season.select('*',t,w,thursdayslots,'*'))<=1, name=cName)
# NFLmodel.update()                           

NFLmodel.update()
#check if proper formulation
NFLmodel.write('test.lp') 

#model solving tuning params:
# NFLmodel.setParam('MIPFocus',0)
NFLmodel.setParam('TimeLimit',30)
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

