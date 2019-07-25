'''
Date: 25 June 2018
Purpose: Dedup files based on entity matching api
Data Integration Assignment 3

SUBMISSION BY: GROUP B
  NAME: Abhishek Shrestha  (Matr. Nummer: 390055)
        Jia Jia            (Matr. Nummer: 389917)
        Syed Salman Ali    (Matr. Nummer: 395898)


<git> https://gitlab.tu-berlin.de/mandir123/DataIntegration</git>
<Summary>  
For the given dataset,           
             TASK 2. Use partioning/ blocking for duplicate detection.
</Summary>

'''

import csv
import py_entitymatching as em
import pandas as pd
import datetime

'''
    Ref. from: https://nbviewer.jupyter.org/github/anhaidgroup/py_entitymatching/blob/rel_0.1.x/notebooks/guides/step_wise_em_guides/Down%20Sampling.ipynb
    The code is ported to python but the logic of downsampling is originally from the above source :)
'''



csv_source = 'D:\TU\_SEM 3\800-6\Data Integration\DataIntegration\Assignments\Assignment 3\Source\inputDB.csv'

#Read the source csv
StartTime = datetime.datetime.now()
print('Started at: '+ str(StartTime))
Source_1 = em.read_csv_metadata(csv_source, low_memory=False) # setting the parameter low_memory to False  to speed up loading.
Len_Source_1 = len(Source_1)
print('Number of records on source Sample: %d' % Len_Source_1)

Source_2 = em.read_csv_metadata(csv_source, low_memory=False) # setting the parameter low_memory to False  to speed up loading. 
Len_Source_1 = len(Source_2)

#Set key
em.set_key(Source_1, 'RecID')
em.set_key(Source_2, 'RecID')

#Start Downsampling
Sample_1, Sample_2 = em.down_sample(Source_1, Source_2, size=10000, y_param=1, show_progress=True)
print('Downsampling is complete...')
print('Number of records on source Sample: %d' % len(Sample_1))
print(len(Sample_1))
print('______________________')
print('Sample Head...')
print(Sample_1.head())
print('______________________')
print('Sample Properties are: ')
em.show_properties(Sample_1)
em.show_properties(Sample_2)

#Blocking using an attribute blocker
at_blocker = em.AttrEquivalenceBlocker()
A_block = at_blocker.block_tables(Sample_1, Sample_2, 
                                    l_block_attr = 'SSN', r_block_attr = 'SSN', 
                                    l_output_attrs= ['SSN'], r_output_attrs= ['SSN'], 
                                    l_output_prefix ='L_', r_output_prefix='R_')
                                    
print('______________________')
print('After Blocking...')
print(A_block.head())
B_block = A_block[A_block.L_RecID != A_block.R_RecID]

#write to outpupt file
with open("D:\\output.csv", 'w') as File:    
    for index, row in B_block.iterrows():
        File.write(str(row['L_RecID'] + ", " + row['R_RecID'] + "\n"))
EndTime = datetime.datetime.now()
print('Ended at:'+ str(EndTime))
duration_seconds  = (EndTime - StartTime).total_seconds()
duration_in_minutes = divmod(duration_seconds, 60)[0]  
print('Total time taken: ' + str(duration_in_minutes))
      