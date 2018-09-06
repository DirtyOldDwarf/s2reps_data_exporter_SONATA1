# coding: utf-8

from __future__ import division
from pymongo import MongoClient
from bson.son import SON
import pprint
import numpy
import csv
import collections


client = MongoClient('localhost', 27017)
db = client.sc2reps
collection = db.basic_db_rev22

# TODO: update exporter explanation and translate to ENG
'''
Wynik procedury to średnie wartości zmiennych dla każdego uczestnika w podziale na 1/4 czasu treningu

Sekwencja agregacji danych:
1. Iteruj po liście PIds.
2. Dla danego PId, pobierz całkowity czas treningu i podziel na 4.
3. Dla danego PId, pobierz listę gier posortowaną wg. daty.
4. Iteruj po liście gier i sumuj liczbę gier, czasy ich trwania (pomijaj krótkie) oraz średnie APMy, gdy wartość sumy
   czasu trwania przekroczy wartość 1/4 czasu treningu zapisz wartości i iteruj dalej.
5. Na koniec zapisz bazę do formatu CSV.
'''

# key = PId, value = [group (1-static;2-random), sex (1-male;2-female)]
pids = {
    '301': ['2','1'],
    '302': ['2','1'],
    '303': ['2','2'],
    '304': ['2','1'],
    '305': ['2','1'],
    '306': ['2','2'],
    '307': ['2','2'],
    '308': ['2','2'],
    '309': ['2','2'],
    '310': ['2','2'],
    '312': ['2','1'],
    '314': ['2','1'],
    '315': ['2','1'],
    '316': ['2','2'],
    '317': ['2','2'],
    '318': ['2','1'],
    '319': ['2','2'],
    '320': ['2','2'],
    '321': ['2','2'],
    '322': ['2','2'],
    '323': ['2','2'],
    '324': ['2','2'],
    '325': ['2','1'],
    '326': ['2','2'],
    '327': ['2','1'],
    '328': ['2','1'],
    '329': ['2','1'],
    '330': ['2','2'],
    '361': ['2','1'],
    '332': ['1','1'],
    '334': ['2','1'],
    '335': ['1','2'],
    '336': ['2','2'],
    '338': ['1','2'],
    '339': ['1','2'],
    '341': ['1','2'],
    '342': ['1','2'],
    '343': ['1','2'],
    '344': ['1','1'],
    '345': ['1','1'],
    '346': ['1','2'],
    '347': ['1','2'],
    '349': ['1','2'],
    '350': ['1','2'],
    '351': ['1','1'],
    '352': ['1','2'],
    '353': ['1','1'],
    '354': ['1','1'],
    '355': ['1','1'],
    '356': ['1','1'],
    '357': ['1','1'],
    '358': ['1','1'],
    '359': ['1','1'],
    '360': ['1','1'],
    '362': ['1','1'],
    '363': ['1','1'],
    '364': ['1','2'],
    '365': ['1','2'],
    '366': ['1','2'],
    '368': ['2','2'],
    '370': ['2','1']
}

difficultyLevels = [
    'Very Easy',
    'Easy',
    'Medium',
    'Hard',
    'Harder',
    'Very Hard',
    'Elite',
    'Cheater 1 (Vision)'
]

source_db_headers = [
        "expansionCC_2",
        "expansionCC_3",
        "expansionCC_4",
        "expansionCC_5",
        "expansionCC_6",
        "expansionCC_7",
        "expansionCC_8",
        "expansionCC_9",
        "expansionCC_10",
        "expansionCC_11",
        "mineralAvailableAverage_2",
        "mineralAvailableAverage_5",
        "mineralAvailableAverage_10",
        "mineralAvailableAverage_15",
        "mineralCollectionRateAverage_2",
        "mineralCollectionRateAverage_5",
        "mineralCollectionRateAverage_10",
        "mineralCollectionRateAverage_15",
        "mineralPeakCollectionRate",
        "mineralSpendAverage_2",
        "mineralSpendAverage_5",
        "mineralSpendAverage_10",
        "mineralSpendAverage_15",
        "resourcesCollectionRateAverage_2",
        "resourcesCollectionRateAverage_5",
        "resourcesCollectionRateAverage_10",
        "resourcesCollectionRateAverage_15",
        "resourcesPeakCollectionRate",
        "resourcesSpendAverage_2",
        "resourcesSpendAverage_5",
        "resourcesSpendAverage_10",
        "resourcesSpendAverage_15",
        "supplyAverageMatch",
        "supplyAverage_2",
        "supplyAverage_5",
        "supplyAverage_10",
        "supplyAverage_15",
        "supplyLimitMatch",
        "supplyLimit_2",
        "supplyLimit_5",
        "supplyLimit_10",
        "supplyLimit_15",
        "supplyMaxTime",
        "supplyMaxValue",
        "upgradeCC_1",
        "upgradeCC_2",
        "upgradeCC_3",
        "upgradeCC_4",
        "upgradeCC_5",
        "upgradeCC_6",
        "upgradeCC_7",
        "upgradeCC_8",
        "upgradeCC_9",
        "upgradeCC_10",
        "upgradeCC_11",
        "upgradeCC_12",
        "upgradeCC_13",
        "upgradeCC_14",
        "upgradeCC_15",
        "upgradeCC_16",
        "upgradeCC_17",
        "upgradeCC_18",
        "upgradeCC_19",
        "vespeneCollectionRateAverage_2",
        "vespeneCollectionRateAverage_5",
        "vespeneCollectionRateAverage_10",
        "vespeneCollectionRateAverage_15",
        "vespenePeakCollectionRate",
        "vespeneSpendAverage_2",
        "vespeneSpendAverage_5",
        "vespeneSpendAverage_10",
        "vespeneSpendAverage_15"
]

database = []

for pid in pids:
    output = {}

    output['PId'] = pid
    output['group'] = pids[pid][0]
    output['sex'] = pids[pid][1]

    # Number of matches played
    output['trainingMatches'] = collection.find({"participantId": pid}).count()

    # Number of gmaes won and lost
    pipeline_winLossRatio = [
        {"$match": {
            "participantId": pid
        }},
        {"$group": {
            "_id": "$matchResult",
            "count": {"$sum": 1}
        }}
    ]
    winLossCount = list(db.basic_db.aggregate(pipeline_winLossRatio))

    tmpWins = next((item for item in winLossCount if item["_id"] == "Win"), 0)

    output['winCount'] = tmpWins['count']
    output['lossCount'] = output['trainingMatches'] - tmpWins['count']
    output['winsRatio'] = float(output['winCount']) / float(output['trainingMatches'])

    # Number of matches on different difficulty level
    pipeline_difficultyLevels = [
        {"$match": {
            "participantId": pid
        }},
        {"$group": {
            "_id": "$aiDifficulty",
            "count": {"$sum": 1}
        }}
    ]
    difficultyLevelsCount = list(db.basic_db.aggregate(pipeline_difficultyLevels))

    for lvl in difficultyLevels:
        tmpDiffLvlCount = next((item for item in difficultyLevelsCount if item["_id"] == lvl), [])

        if len(tmpDiffLvlCount) > 0:
            output['trainingMatchesLvl_'+lvl.replace(' ','_').replace('(','').replace(')','')] = tmpDiffLvlCount['count']
        else:
            output['trainingMatchesLvl_' + lvl.replace(' ', '_').replace('(','').replace(')','')] = 0

    # Total matches real time
    pipeline_trainingTime = [
        {"$match": {
            "participantId": pid
        }},
        {"$group": {
            "_id": "null",
            "total": {"$sum": "$matchLengthRealTime"}
        }}
    ]
    output['trainingTime'] = list(db.basic_db.aggregate(pipeline_trainingTime))[0]['total']

    # Calculate values for whole training time and in quaters
    quarterTime = output['trainingTime'] / 4

    # Prep the dict for global sums storage
    countersGlobal = dict((el,0) for el in source_db_headers)

    # Set postfixes for quater data output
    postfixDataKeys = ["_q1","_q2","_q3","_q4"]

    matchesForPId = collection.find({"participantId": pid}).sort("matchStartDateTime", 1)

    # Init time counters for aggregation of game time, games and wins number
    qCount = 0
    qTimeCounter = 0
    qMatchesCounter = 0
    qWinsCounter = 0
    qAPMCounter = 0

    # Init the dict with db headers, each key has a corresponding key in match data set
    qCounters = dict((el, [0,0]) for el in source_db_headers)

    for idx, match in enumerate(matchesForPId):

        if qTimeCounter == 0 or qTimeCounter < quarterTime:
            qTimeCounter += match['matchLengthRealTime']
            qMatchesCounter += 1
            qWinsCounter += 1 if match['matchResult'] == "Win" else 0
            qAPMCounter += match['avgAPM']

            for var_name in source_db_headers:
                if var_name in match.keys():
                    qCounters[var_name][0] += 0 if match[var_name] == None else match[var_name]
                    qCounters[var_name][1] += 0 if match[var_name] == None else 1

        if (qTimeCounter > quarterTime and qCount < 3) or idx == matchesForPId.count() - 1:
            # zapisz zebrane dane (czas i liczbę gier)
            output['totalTime'+postfixDataKeys[qCount]] = qTimeCounter
            output['averageAMPs'+postfixDataKeys[qCount]] = qAPMCounter / qMatchesCounter
            output['totalMatches'+postfixDataKeys[qCount]] = qMatchesCounter
            output['ratioWins'+postfixDataKeys[qCount]] = qWinsCounter / qMatchesCounter

            # DISABLED: Store matches number for each var
            # Divide each counter value by number of matches in quater
            # Rename keys by adding quarter postfix
            for x, y in qCounters.items():
                # qCounters[x + '_matchesCount'] = y[1]
                qCounters[x] = y[0] / y[1] if y[1] > 0 else 0
                qCounters[x + postfixDataKeys[qCount]] = qCounters.pop(x)

            # Mearge qCounters to output
            output.update(qCounters)

            # Reset counters, inc qCount
            qCounters = dict((el, [0,0]) for el in source_db_headers)
            qTimeCounter = 0
            qMatchesCounter = 0
            qWinsCounter = 0
            qAPMCounter = 0
            qCount += 1


    # Add sorted result to the output DB and reser qCount back to 0
    database.append(collections.OrderedDict(sorted(output.iteritems(), key=lambda (k,v): (v,k))))
    qCount = 0
    print("=== Done: "+str(pid))

keys = sorted(output.keys())
with open('sc2reps_rev22_aggregates_SONATA1.csv', 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(database)

print ("All done")