
from tabulate import tabulate
import json
import re
from mpi4py import MPI


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    # read in the JSON file
    # Open the JSON file for reading
    with open('sal.json', 'r') as f:
        # Load the JSON data from the file
        sal = json.load(f)
        
    new_sal = {}
    check_list = []
    for key in sal.keys():
        if sal[key]['gcc'][1] == 'g':  # needs to be modified 7gdar, 8acte, and 9oter
            new_sal[key] = sal[key]['gcc'].lower()
            check_list.append(sal[key]['gcc'].lower())

    # to go through sal and merge suburbs of the same name
    # in different states
    # and flter out those not in greater city

    sal_new = {}
    state_list = []
    pattern1 = r'^(.*?)\s*\((.*?)\)$'
    pattern2 = r'^(.*?[\w-]*)\s*-\s*(.*?)$' 

    for key in sal.keys():
        check = sal[key]['gcc'][1] == 'g'
        if check:
            match = re.match(pattern1, key)
            if not match:
                if key not in sal_new.keys():
                    sal_new[key] = {'none':sal[key]}
            else:
                suburb = match.group(1)
                state = match.group(2)
            
                check2 = re.match(pattern2, state)
                if not check2:
                    state = state
                else:
                    state = check2.group(2)
                    
                state_list.append(state)
                
                if suburb not in sal_new.keys():
                    sal_new[suburb] = {state: sal[key]}
                else:
                    sal_new[suburb].update({state:sal[key]})
    
    # Broadcast the modified data to all other nodes
    comm.bcast(sal_new, root=0)

else:
    sal_new = comm.bcast(None, root=0)
    
    

def processTwits(twi, sal_new):
    twi_dic = {}
    usr_count = {}
    
    conversion_dic = {
    ' melbourne': 'vic.',
    ' south australia': 'sa',
    ' tasmania': 'tas.',
    ' victoria': 'vic.',
    ' northern territory': 'nt',
    ' new south wales': 'nsw',
    ' queensland': 'qld',
    ' sydney': 'nsw',
    ' western australia': 'wa'
    }

    for i in range(len(twi)):
        location = twi[i]['includes']['places'][0]['full_name'].lower().split(",")
    
        if len(location) > 1:
            if location[1] in conversion_dic.keys():
                location[1] = conversion_dic[location[1]]
        new_dic = {'usr': twi[i]['data']['author_id'], 'loc':location}
        twi_dic[twi[i]['_id']] = new_dic
                

    for twits in twi_dic:
    
        usr = twi_dic[twits]['usr']
        if usr not in usr_count.keys():
            usr_count[usr] = {}
            usr_count[usr]['total_twits'] = 1
            usr_count[usr]['great_twits'] = 0
            usr_count[usr]['cities'] = []
        else:
            usr_count[usr]['total_twits'] += 1
        
        loc = twi_dic[twits]['loc']
        if len(loc) == 1:
            if loc[0] in sal_new.keys():
                twi_dic[twits]['gcc'] = sal_new[loc[0]]['none']['gcc']
                usr_count[usr]['great_twits'] += 1
                usr_count[usr]['cities'].append(sal_new[loc[0]]['none']['gcc'])
            else:
                twi_dic[twits]['gcc'] = 'not found'
        else:
            if loc[0] in sal_new.keys():
                if loc[1] in sal_new[loc[0]].keys():
                    twi_dic[twits]['gcc'] = sal_new[loc[0]][loc[1]]['gcc']
                    usr_count[usr]['great_twits'] += 1
                    usr_count[usr]['cities'].append(sal_new[loc[0]][loc[1]]['gcc'])
                elif (len(sal_new[loc[0]].keys()) == 1) & ('none' in sal_new[loc[0]].keys()):
                    twi_dic[twits]['gcc'] = sal_new[loc[0]]['none']['gcc']
                    usr_count[usr]['great_twits'] += 1
                    usr_count[usr]['cities'].append(sal_new[loc[0]]['none']['gcc'])
                else:
                    twi_dic[twits]['gcc'] = 'not found'
            else:
                twi_dic[twits]['gcc'] = 'not found'
    
    return twi_dic, usr_count
    
    
# read in the twitter JSON file
with open('tinyTwitter.json', 'r', encoding='utf-8') as f:
    # Load the JSON data from the file
    twi = json.load(f)


# split the twi data into chunks for each node
chunks = [[] for _ in range(size)]
for i, tweet in enumerate(twi):
    chunks[i % size].append(tweet)


# process the twitter data at each node
twi_dic, usr_count = processTwits(chunks[rank], sal_new)


# send the results to root node for aggregation
if rank == 0:
    twi_dic_agg = {}
    usr_count_agg = {}
    
    # merge results from each node other than root
    for i in range(1, size):
        twi_dic_i, usr_count_i = comm.recv(source=i)
        
        for tag in twi_dic_i:
            # there won't be 2 tweets shareing the same tweet id
            twi_dic_agg[tag] = twi_dic_i[tag]
            
        for user_id in usr_count_i:
            if user_id in usr_count_agg:
                usr_count_agg[user_id]['total_twits'] += usr_count_i[user_id]['total_twits']
                usr_count_agg[user_id]['great_twits'] += usr_count_i[user_id]['great_twits']
                usr_count_agg[user_id]['cities'].extend(usr_count_i[user_id]['cities'])
            else:
                usr_count_agg[user_id] = usr_count_i[user_id]
    
    # merge results from root node            
    for tag in twi_dic: 
        twi_dic_agg[tag] = twi_dic[tag]
        
    for user_id in usr_count:
        if user_id in usr_count_agg:
            usr_count_agg[user_id]['total_twits'] += usr_count[user_id]['total_twits']
            usr_count_agg[user_id]['great_twits'] += usr_count[user_id]['great_twits']
            usr_count_agg[user_id]['cities'].extend(usr_count[user_id]['cities'])
        else:
            usr_count_agg[user_id] = usr_count[user_id]
            
else:
    # other nodes send their results to root node
    comm.send((twi_dic, usr_count), dest=0)


if rank == 0:

    # '1gsyd', '5gper', '4gade', '2gmel', '7gdar', '3gbri', '6ghob'
    twits_count = {
        '1gsyd': 0,
        '2gmel': 0,
        '3gbri': 0,
        '4gade': 0,
        '5gper': 0,
        '6ghob': 0,
        '7gdar': 0
        
    }

    for twits in twi_dic_agg:
        if twi_dic_agg[twits]['gcc'] in twits_count.keys():
            twits_count[twi_dic_agg[twits]['gcc']] += 1

    print(tabulate(twits_count.items(), headers=['Greater Capital City','Number of Tweets Made']))


    # Sort the dictionary keys based on the subkey value using a lambda function
    sorted_keys = sorted(usr_count_agg, key=lambda x: usr_count_agg[x]["total_twits"], reverse=True)
    top_10_usr = sorted_keys[:10]

    top_10_counts = []
    for usr in top_10_usr:
        top_10_counts.append(usr_count_agg[usr]['total_twits'])

    output1 = {
        'rank':[1,2,3,4,5,6,7,8,9,10],
        'user_id': top_10_usr,
        'counts': top_10_counts
    }

    print(tabulate(output1, headers='keys'))


    for keys in usr_count_agg:
        unique_cities = list(set(usr_count[keys]['cities']))
        unique_cnts = len(unique_cities)
        usr_count_agg[keys]['unqiue_cities'] = unique_cities
        usr_count_agg[keys]['unique_counts'] = unique_cnts
        usr_count_agg[keys]['summary'] = str(unique_cnts) + " (" + str(usr_count_agg[keys]["great_twits"]) + " tweets - " + str(unique_cities) + ")"
        

    sorted_dict_new = sorted(usr_count_agg.items(), key=lambda x: (x[1]["unique_counts"], x[1]["great_twits"]), reverse=True)
    sorted_dict_new = dict(sorted_dict_new)

    final_output = dict(list(sorted_dict_new.items())[0: 10])

    output_dic = {
        'rank': [1,2,3,4,5,6,7,8,9,10],
        'id': [],
        'summary': []
    }

    for keys in final_output:
        output_dic['id'].append(keys)
        output_dic['summary'].append(final_output[keys]['summary'])
        
    print(tabulate(output_dic, headers='keys'))