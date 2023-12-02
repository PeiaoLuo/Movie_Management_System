import re

loc = "D:\MMS_prj\Movie_sys\generation\gen.txt"
pattern_table = r"insert into (.*)"
pattern_value = r"values\('([^']+(?:','[^']+)*)'\);"
pattern_box = r"values\('(.*)',(.*)\);"

def sql2dict(dict_ls,loc=loc):
    value = []
    key = ""
    with open(loc, 'r', encoding='utf-8') as fp:
        while 1:
            line = fp.readline()
            if line:
                if line=='\n':
                    continue
                else:
                    match_result = re.match(pattern_table, line)
                    if match_result:
                        key = match_result.group(1)
                    else:
                        if key!='movie_box':
                            match_result = re.match(pattern_value, line)
                            if match_result:
                                value = list(match_result.group(1).split("','"))
                                dict_ls.append({key:value})
                        else:
                            match_result = re.match(pattern_box, line)
                            if match_result:
                                value = list(match_result.groups())
                                dict_ls.append({key:value})
                            else:
                                print('Invalid input:')
                                print(line)
            else:
                break