import pandas as pd
from collections import defaultdict
df = pd.read_csv('subid_homeid.csv')

my_dict = defaultdict(list)


for index, row in df.iterrows():
    
    homeid = int(row['homeid'])
    subid = int(row['subid'])
    
    my_dict[homeid].append(subid)
    

for home, sub in my_dict.items():
    print({home},{sub})