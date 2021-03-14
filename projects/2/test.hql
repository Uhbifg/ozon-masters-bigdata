add file  projects/2/predict.py;
add file /opt/conda/envs/dsenv/bin/python3;
select transform(id) using 'projects/2/predict.py' as (id) from hw2_test where if1==1;
