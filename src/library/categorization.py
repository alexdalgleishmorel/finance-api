def categorization_report(dataframe, fuzzy_successes, unknown_rankings):
    total = 0
    success = 0
    unknown = 0

    for index in dataframe.index:
        if dataframe['Category'][index] != "UNKNOWN":
            success += 1
        else:
            unknown += 1
        total += 1
    
    print(f"Total successful matches: {success}/{total} --> {str(round((success/total)*100, 2))}%")
    print(f"Total unknowns: {unknown}/{total} --> {str(round((unknown/total)*100, 2))}%")
    print("Fuzzy Successes: %s" % fuzzy_successes)
    rank_array = []
    for key in unknown_rankings:
        rank_array.append((key, unknown_rankings[key]))
    rank_array.sort(key = lambda x: x[1])
    rank_array.reverse()
    top_five = rank_array[0:5]
    for tuple in top_five:
        print(tuple)
