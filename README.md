# Data Curation

Daily Kurly 유저 데이터 분석 및 큐레이션 프로그램입니다.

## 1. 시작하기

```sh
pip install requirements.txt
```

`MongoUrl = 'mongodb+srv://{username}:{password}@{clusterURI}/'`에서 username, password, clusterURI를 입력합니다. (컬리 주최측을 위해 해당 부분은 daily-kulry-api repo의 `.env.example` 파일에 적어뒀습니다.)

이후 `usertaste('lee123')`에서 'lee123' 대신 확인하고 싶은 유저의 id를 입력하면 됩니다.

## 2. 실행 결과

![curation result](https://user-images.githubusercontent.com/63287638/186352063-3d8f0070-27ca-4964-867d-07acb0b5081b.png)
