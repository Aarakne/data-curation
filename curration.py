# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 22 18:54:37 2022

@author: JIH
"""

from datetime import timedelta
from datetime import datetime
import pymongo
import pandas as pd
from pymongo import MongoClient
import datetime
import re
import string
import random
import certifi

client = MongoClient()
# point the client at mongo URI
MongoUrl = 'mongodb+srv://{username}:{password}@{clusterURI}/'
client = MongoClient(MongoUrl,tlsCAFile=certifi.where())
# select database
db = client['daily-kurly']

# select the collection within the database
test = db.products
test = pd.DataFrame(list(test.find()))
df_products = test

test = db.posts
test = pd.DataFrame(list(test.find()))
df_posts = test

test = db.users
test = pd.DataFrame(list(test.find()))
df_users = test

test = db.producttracinglogs
test = pd.DataFrame(list(test.find()))
df_logs = test

test = db.purchases
test = pd.DataFrame(list(test.find()))
df_purchases = test

### data cleansing
users = df_users[['_id','username','likedPosts','posts']]

# df_posts products
df_posts.info()
cc = df_posts['usedProducts'].apply(lambda x: pd.Series(x))
df_posts[list(range(len(cc.columns)))] = cc[list(range(len(cc.columns)))] 


### 취향분석 함수
def usertaste(username:str):
    if (len(df_purchases[df_purchases['username']==username]))|(len(df_logs[df_logs['username']==username]))==0:
        print('No User Data\nquit.')
        return
    df_taste = pd.DataFrame(columns = ['username','product','brand','cat2','deliverys','brands','prices','rates','c_cat1','c_cat2','cu1','cu2','cu3','cu4','cu5'], index=range(1))
    df_taste['username'][0] = username
    ### 구매내역 분석
    delivery_u = ''
    user_purchase = df_purchases[df_purchases['username']==username]
    purchase_product = pd.DataFrame(data=list(user_purchase['products'].values))
    purchase_list = list(user_purchase['products'].values)
    purchase_product = []
    for i in range(len(purchase_list)):
        for a in range(len(purchase_list[i])):
            purchase_product.append(purchase_list[i][a])
    purchase_product = pd.DataFrame(data = purchase_product)
    data1 = pd.merge(purchase_product, df_products, left_on=0, right_on='_id', how='left')
    a,b = data1.groupby('name')['name'].count().idxmax(), data1.groupby('name')['name'].count().max()
    print('1. [%s]의 구매내역 분석'%username)
    if b/len(data1)>0.05:
        print('>> [%s]을 재구매할 확률이 높음'%a)
        df_taste['product'][0] = data1.groupby('name').count().reset_index()['name'][:3].tolist()
    a,b = data1.groupby('brand')['brand'].count().idxmax(), data1.groupby('brand')['brand'].count().max()
    if b/len(data1)>0.1:
        print('>> [%s]에 대한 브랜드 선호도가 강함'%a)
    df_taste['brand'][0] = data1.groupby('brand').count().reset_index()['brand'][:3].tolist()
    a,b = data1.groupby('cat2')['cat2'].count().idxmax(), data1.groupby('cat2')['cat2'].count().max()
    if b/len(data1)>0.15:
        print('>> [%s]에 대한 카테고리 선호도가 강함'%a)
        df_taste['cat2'][0] = data1.groupby('cat2').count().reset_index()['cat2'][:3].tolist()
    a,b = data1.groupby('delivery')['delivery'].count().idxmax(), data1.groupby('delivery')['delivery'].count().max()
    if b/len(data1)>0.8:
        print('>> 샛별배송 상품에 민감하게 반응')
        df_taste['deliverys'][0] = b/len(data1)
    else:
        df_taste['deliverys'][0] = False
    ### log 분석
    user_logs = df_logs[df_logs['username']==username]
    ## 상품 비교
    logs1 = user_logs[['productId','postId']]
    logs1 = pd.merge(logs1, df_posts, left_on='postId', right_on='_id', how='left')
    usedProducts_list = list(logs1['usedProducts'].values)
    used_Products = []
    for i in range(len(usedProducts_list)):
        for a in range(len(usedProducts_list[i])):
            used_Products.append(usedProducts_list[i][a])
    used_Products = pd.DataFrame(data = used_Products)
    used_Products = pd.merge(used_Products, df_products, left_on=0, right_on='_id', how='left')
    used_Products = pd.merge(used_Products, df_products, left_on='relatedProduct', right_on='_id', how='left')
    used_Products['select'] = 'nan'
    used_Products['select'].loc[used_Products[used_Products['_id_x'].isin(logs1['productId'].tolist())].index.tolist()] = 'x'
    used_Products['select'].loc[used_Products[used_Products['_id_y'].isin(logs1['productId'].tolist())].index.tolist()] = 'y'
    used_Products = used_Products[used_Products['select']!='nan'].reset_index(drop=True)
    used_Products = used_Products.assign(price='',rate='',brand='')
    for i in range(len(used_Products)):
        select = used_Products['select'].iloc[i]
        if select == 'x':
            select1 = 'y'
        else:
            select1 = 'x'
        if (used_Products['sellingPrice_%s'%select][i]<=used_Products['sellingPrice_%s'%select1][i]):
            used_Products['price'][i] = 'O'
        if (used_Products['discountRate_%s'%select][i]>=used_Products['discountRate_%s'%select1][i]):
            used_Products['rate'][i] = 'O'
        used_Products['brand'][i] = used_Products['brand_%s'%select][i]
    used_Products['price'].loc[used_Products[used_Products['price']==''].index.tolist()] = 'X'
    used_Products['rate'].loc[used_Products[used_Products['rate']==''].index.tolist()] = 'X'
    print('\n2. Daily Kurly 로그 분석')
    a,b = used_Products.groupby('brand')['brand'].count().idxmax(), used_Products.groupby('brand')['brand'].count().max()
    if b/len(used_Products)>0.15:
        print('>> 브랜드 민감도가 높음')
        df_taste['brands'][0] = b/len(used_Products)
    if len(used_Products[used_Products['price']=='O'])/len(used_Products)>=0.7:
        print('>> 가격 민감도가 높음')
        df_taste['prices'][0] = len(used_Products[used_Products['price']=='O'])/len(used_Products)
    if len(used_Products[used_Products['rate']=='O'])/len(used_Products)>=0.7:
        print('>> 할인율 민감도가 높음')
        df_taste['rates'][0] = len(used_Products[used_Products['rate']=='O'])/len(used_Products)
    ## 요리 취향
    cat1_c,cat2_c = logs1.groupby('category1')['category1'].count().idxmax(), logs1.groupby('category2')['category2'].count().idxmax()
    print('[%s]는 [%s]의 [%s]요리를 좋아한다.'%(username,cat1_c,cat2_c))
    df_taste['c_cat1'][0] = cat1_c
    df_taste['c_cat2'][0] = cat2_c
    df_taste.fillna('False', inplace=True)
    ccc1 = df_taste.copy()
    ### 큐레이션
    if len(ccc1.loc[:,(ccc1==1).any(axis=0)].iloc[0]) >= 1:
        importance = ccc1.loc[:,(ccc1==1).any(axis=0)].iloc[:,0].name[:-1]
    else:
        importance = 'price'
    if (importance == 'rate')|(importance == 'brand'):
        sort = False
    else:
        sort = True
    if importance == 'rate':
        importance1 = 'discountRate'
    elif importance == 'price':
        importance1 = 'sellingPrice'
    else:
        importance1 = 'brand'
    morning_delivery = ccc1['deliverys'][0]
    print('\n\n개인화 포커싱 : [%s]는 [%s]를 중점으로 쇼핑을 한다.\n\n3. 상품 큐레이션'%(username,importance))
    print('[자주 산 상품]')
    df_products[df_products['name'].isin(ccc1['product'][0])]['name'].tolist()
    ccc1['cu1'][0] = df_products[df_products['name'].isin(ccc1['product'][0])]['name'].tolist()
    print('>> ',ccc1['cu1'][0])
    print('[선호 브랜드 상품]')
    if 'brand' == importance:
        ccc1['cu2'][0] = df_products[(df_products['brand'].isin(ccc1['brand'][0]))&(df_products['delivery']==morning_delivery)][['name','avgsold']].sort_values(['avgsold'], ascending=False)['name'][:3].tolist()
    else:
        ccc1['cu2'][0] = df_products[(df_products['brand'].isin(ccc1['brand'][0]))&(df_products['delivery']==morning_delivery)][['name',importance1]].sort_values([importance1], ascending=sort)['name'][:3].tolist()
    print('>> ',ccc1['cu2'][0])
    print('[선호 카테고리 상품]')
    if importance == 'brand':
        top_list = df_products[(df_products['cat2'].isin(ccc1['cat2'][0]))&(df_products['delivery']==morning_delivery)][['name',importance1,'avgsold']].sort_values(['avgsold'], ascending=sort)['brand'][:5].tolist()
        ccc1['cu3'][0] = df_products[(df_products['cat2'].isin(ccc1['cat2'][0]))&(df_products['delivery']==morning_delivery)&(df_products['brand'].isin(top_list))][['name','avgsold']].sort_values(['avgsold'], ascending=sort)['name'][:3].tolist()
    else:
        ccc1['cu3'][0] = df_products[(df_products['cat2'].isin(ccc1['cat2'][0]))&(df_products['delivery']==morning_delivery)][['name',importance1]].sort_values([importance1], ascending=sort)['name'][:3].tolist()
    print('>> ',ccc1['cu3'][0])
    print('[사용자 맞춤 상품1 {%s}]'%importance)
    if importance == 'brand':
        print('%s님이 좋아할 맞춤 브랜드 상품'%username)
        ccc1['cu4'][0] = df_products[(df_products['brand'].isin(ccc1['brand'][0]))&(df_products['delivery']==morning_delivery)][['name','avgsold']].sort_values(['avgsold'], ascending=False)['name'][:3].tolist()
        print('>> ',ccc1['cu4'][0])
    elif importance == 'rate':
        print('%s님을 위한 특가 상품'%username)
        ccc1['cu4'][0] = df_products[(df_products['delivery']==morning_delivery)][['name',importance1]].sort_values([importance1], ascending=sort)['name'][:3].tolist()
        print('>> ',ccc1['cu4'][0])
    else:
        print('%s님을 위한 최저가 상품'%username)
        ccc1['cu4'][0] = df_products[(df_products['delivery']==morning_delivery)][['name',importance1]].sort_values([importance1], ascending=sort)['name'][:3].tolist()
        print('>> ',ccc1['cu4'][0])
    print('\n4. 사용자 맞춤 요리 큐레이션]')
    print('[%s님]이 좋아하는 [%s>%s] 인기요리'%(username,ccc1['c_cat1'][0],ccc1['c_cat2'][0]))
    ccc1['cu5'][0] = df_posts[(df_posts['category1']==ccc1['c_cat1'][0])&((df_posts['category2']==ccc1['c_cat2'][0]))][['title','hitCount']].sort_values(['hitCount'],ascending=False)['title'].tolist()
    print('>> ',ccc1['cu5'][0])
    return ccc1

taste_result = usertaste('lee123')
