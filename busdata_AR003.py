from turtle import update
from typing import ItemsView
import time
from operator import index, mod
from re import A, S
from urllib.parse import urlencode, unquote
from bs4 import BeautifulSoup
import pandas as pd
import requests
from pyproj import Transformer


class Merge():
    def __init__(self) -> None:
        self.api_get_bus_route("http://apis.data.go.kr/6280000/busRouteService/getBusRouteNo")
        self.api_busStationByRoute("http://apis.data.go.kr/6280000/busRouteService/getBusRouteSectionList")

    def get_text(self, item, key):
        value = item.find(key)
        if value is None:
            return None

        return value.get_text()

    def switch(self, bustype):
        #### routeTypeCd 가 11 일 때 DB를 조회해보면 BT004일때도 있고 BT008일때도 있다.###
        bus_code = {"1": "BT002", "2": "BT001", "3": "BT006", "4": "BT005", "5": "리무진", "6": "BT011", "7": "BT008",
                    "8": "BT007", "9": "BT001"}.get(bustype, "BT")
        # 인천 API 제공 정보: 노선 유형 (1: 지선형, 2: 간선형, 3: 좌석형, 4: 광역형, 5: 리무진, 6: 마을버스,  7: 순환형, 8: 급행간선, 9: 지선(순환)
        # 네이버스 DB 제공 정보: 버스_타입 (BT001:간선, BT002:지선, BT003:일반, BT004:직행,BT005:광역,BT006:좌석,BT007:급행, BT008:순환, BT009:공항, BT010:시외, BT011:마을, BT012:농어촌, BT013:공영)
        return bus_code

    def api_get_bus_route(self, url):
        print(3)
        api_key = 'cGu7HjX8fvGLnh7Gsd0V37acUM9nUS6GmbPFFhsEpOx1o5cjtKODHcuH4%2B5z0Kz8Ky%2BGsahQ%2BKhMHrEC3wmb3g%3D%3D'
        api_key_decode = requests.utils.unquote(api_key)
        global route_list
        route_list = []
        row_route = []
        # 0~9까지 검색하여 노선 list 생성
        for i in range(0, 10):
            params = {'serviceKey': api_key_decode, 'pageNo': 1, 'numOfRows': 1000, 'routeNo': i}

            response = requests.get(url, params=params)
            response.encoding = 'euc-kr'

            soup = BeautifulSoup(response.content, 'lxml-xml')

            time.sleep(0.02)
            items = soup.find_all("itemList")
            j = 0
            for item in items:
                ROUTETYPE = self.get_text(item, "ROUTETPCD")
                ROUTEID = self.get_text(item, "ROUTEID")
                AREA = self.get_text(item, "ADMINNM")
                BUSLINENUM = self.get_text(item, "ROUTENO")
                # BUSLINEABRV= self.get_text(item,"busRouteAbrv")
                COMPANYNAME = self.get_text(item, "companyName")
                STARTPOINT = self.get_text(item, "ORIGIN_BSTOPNM")
                ENDPOINT = self.get_text(item, "DEST_BSTOPNM")
                FIRSTTIME = self.get_text(item, "FBUS_DEPHMS")
                ENDTIME = self.get_text(item, "LBUS_DEPHMS")
                TURNING = self.get_text(item, "TURN_BSTOPNM")
                PEEKALLOC = self.get_text(item, "MIN_ALLOCGAP")  # 평일 최소 배차시간
                NPEEKALLOC = self.get_text(item, "MAX_ALLOCGAP")  # 평일 최대 배차시간
                ROUTETYPE = self.switch(ROUTETYPE)

                api_dic_bus_route[ROUTEID] = [STARTPOINT, ENDPOINT, FIRSTTIME, ENDTIME]

                if ROUTEID not in route_list:
                    route_list.append(ROUTEID)
                if len(FIRSTTIME) == 3:
                    FIRSTTIME = "0" + FIRSTTIME
                if len(ENDTIME) == 3:
                    ENDTIME = "0" + ENDTIME
                j = j + 1
                # print(ROUTEID, i, STARTPOINT, j)
                row_route.append({
                    "type": ROUTETYPE,
                    "route_id": ROUTEID,
                    "city": "AR003",
                    "city_code": "",
                    "area": AREA,
                    "bus_no": BUSLINENUM,  # 버스 노선 번호
                    "bus_no_title": BUSLINENUM,
                    "bus_no_sub_title": '',
                    "bus_company": COMPANYNAME,
                    "start_point": STARTPOINT,
                    "end_point": ENDPOINT,
                    "turning": TURNING,
                    "start_time": FIRSTTIME[0:2] + ":" + FIRSTTIME[2:4],
                    "end_time": ENDTIME[0:2] + ":" + ENDTIME[2:4],
                    "weekdays_term": PEEKALLOC,
                    "weekend_term": NPEEKALLOC,
                    "holiday_term": '',
                    "service_yn": 'Y'
                })
        print(len(route_list))
        global df_bus_route
        df_bus_route = pd.DataFrame(row_route)
        df_bus_route.to_csv("인천 버스 노선.csv", mode='w', )
        # self.compare(db_dic_bus_route, api_dic_bus_route, "노선", "route_id", df_bus_route)

    def korean_to_be_englished(self, korean_word):
        # 초성 리스트. 00 ~ 18
        CHOSUNG_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
        r_lst = []
        for w in list(korean_word.strip()):
            if '가' <= w <= '힣':
                ch1 = (ord(w) - ord('가')) // 588
                r_lst.append(CHOSUNG_LIST[ch1])  # JUNGSUNG_LIST[ch2], JONGSUNG_LIST[ch3]])
            else:
                r_lst.append(w)
        # print(r_lst)
        return "".join(r_lst)

    # def switch_bessel_to_gps(self,POSx,POSy):
    #     transformer = Transformer.from_crs("epsg:2097", "epsg:4326")
    #     answer = list(transformer.transform(POSx, POSy))
    #     return answer

    # 노선별 정류장
    def api_busStationByRoute(self, url):
        global route_list
        global api_arsid_list
        api_arsid_list = []
        api_key = 'cGu7HjX8fvGLnh7Gsd0V37acUM9nUS6GmbPFFhsEpOx1o5cjtKODHcuH4%2B5z0Kz8Ky%2BGsahQ%2BKhMHrEC3wmb3g%3D%3D'
        api_key_decode = requests.utils.unquote(api_key)
        row_bus_and_stop = []
        row_bus_stop = []
        for i in range(len(route_list)):
            params = {'serviceKey': api_key_decode, 'pageNo': 1, 'numOfRows': 1000, 'routeId': route_list[i]}
            # print("노선별 정류장 TEST: " , i, route_list[i])
            response = requests.get(url, params=params)
            response.encoding = 'euc-kr'

            soup = BeautifulSoup(response.content, 'lxml-xml')

            items = soup.find_all("itemList")
            # api_busid_list에서 가져온 busRouteId에 대한 노선별 정류소 정보 조회
            time.sleep(0.5)
            for item in items:
                BUSROUTEID = self.get_text(item, "ROUTEID")
                STATIONSEQ = self.get_text(item, "BSTOPSEQ")  # 노선 정류소 순번
                STATIONNM = self.get_text(item, "BSTOPNM")
                ARSID = self.get_text(item, "SHORT_BSTOPID")
                STATIONID = self.get_text(item, "BSTOPID")
                GPSX = self.get_text(item, "POSX")  # GPS X 좌표
                GPSY = self.get_text(item, "POSY")  # GPS Y 좌표
                # GPSX,GPSY = self.switch_bessel_to_gps(GPSX, GPSY)
                api_dic_bus_stop[STATIONID] = STATIONNM
                # print(STATIONSEQ, BUSROUTEID, GPSX, GPSY, STATIONNM)
                if "(경유)" in STATIONNM:
                    SERVICE_YN = "N"
                elif "(가상)" in STATIONNM:
                    SERVICE_YN = "N"
                elif "(미정차)" in STATIONNM:
                    SERVICE_YN = "N"
                elif "(미경유)" in STATIONNM:
                    SERVICE_YN = "N"
                elif "(중)" in STATIONNM:
                    SERVICE_YN = "N"
                else:
                    SERVICE_YN = "Y"
                row_bus_and_stop.append({
                    "ROUTE_ID": BUSROUTEID,  # 노선 ID
                    "STATION_ID": STATIONID,  # 노드 ID
                    "STATION_ORDER": STATIONSEQ,  # 노선 정류소 순번
                    "정류소명": STATIONNM,  # 정류소 명
                    "AREA": "AR003",
                })
                row_bus_stop.append({
                    "STATION_NM": STATIONNM,  # 정류소 명
                    "GPS_X": GPSX,  # GPS X 좌표
                    "GPS_Y": GPSY,  # GPS Y 좌표
                    "MOBILE_NO": ARSID,  # 정류소 번호
                    "AREA": "AR003",
                    "CITY_CODE": "",
                    "STATION_NM_PARSE": self.korean_to_be_englished(STATIONNM),
                    "STATIONTYPE": '일반',
                    "DIRECTION": '',
                    "SERVICE_YN": SERVICE_YN,
                    "API_ID": '1',
                    "STATION_ID": STATIONID  # 노드 ID
                })
                # print(row_bus_stop)
                # 노선별 정류장에서 받은 ARSID를 list에 담아서 다음 함수에서 ARSID를 넘겨서 getStationByUid에 arsId를 넣는 방식으로 개발 진행
                # ARSID가 없는 경우를 해결해야함
            # api호출 데이터와 DB데이터 정류소 순번 메기는 규칙이 달라서 표시 필요
            # DB에 데이터 다 넣은 후 AREA 셀이 비어있는 행을 모두 지워준다.
            row_bus_and_stop.append({"정류소명": 'Terminal', "노선고유번호": i, "표준버스정류장ID": i})
        global df_bus_and_stop
        df_bus_and_stop = pd.DataFrame(row_bus_and_stop)
        # df_bus_and_stop.to_csv("인천 버스 노선별 정류장.csv", mode='w',)

        global df_bus_stop
        df_bus_stop = pd.DataFrame(row_bus_stop)
        df_bus_stop.to_csv("인천 버스 정류장.csv", mode='w', )
        # self.compare(db_dic_bus_stop, api_dic_bus_stop, "정류장", "STATION_ID", df_bus_stop)

    def compare(self, db_dic, api_dic, key, keyword, df):
        global update_data
        global delete_data
        global insert_data
        self.key = key
        self.keyword = keyword
        self.df = df
        # print(api_dic)
        # print(len(api_dic))
        # print(key,keyword)
        update_data = {}  # 정류장 이름에 대해 업데이트만 필요한 데이터
        delete_data = []
        insert_data = {}

        for x, y in db_dic.items():
            if x in api_dic.keys():
                if y != api_dic[x]:
                    # print("업데이트할 데이터: ", y, "------->" ,api_dic[x], "(",x,")")
                    # update_data.append([x,y]) # 리스트 형식으로 추가 할 경우
                    update_data[x] = api_dic[x]  # 딕셔너리로 추가할 경우 db의 xy를 업데이트 하면 안되고, api의 x,y를 업데이트 해야함
            else:
                # print("삭제할 데이터: " , x, y)
                delete_data.append([x, y])

        for i, j in api_dic.items():
            if i not in db_dic.keys():
                # print("추가할 데이터: ", i, j)
                # insert_data.append([i,j]) # 리스트 형식으로 추가 할 경우
                insert_data[i] = j  # 딕셔너리로 추가할 경우

        # 테스트 출력
        print("@update: ", len(update_data), "@delete: ", len(delete_data), "@insert: ", len(insert_data))
        self.write(update_data, delete_data, insert_data, key, keyword, df)

    def write(self, update_data, delete_data, insert_data, key, keyword, df):
        global doc
        update_worksheet = doc.add_worksheet(title='[API/UPDATE] ' + key, rows=10000, cols=50)
        delete_worksheet = doc.add_worksheet(title='[API/DELETE] ' + key, rows=10000, cols=50)
        insert_worksheet = doc.add_worksheet(title='[API/INSERT] ' + key, rows=10000, cols=50)

        if key == "노선":
            update_worksheet.insert_row(
                ['type', 'route_id', 'city', 'city_code', 'area', 'bus_no', 'bus_no_title', 'bus_no_sub_title',
                 'bus_company', 'start_point', 'end_point', 'turning', 'start_time', 'end_time', 'weekdays_term',
                 'weekend_term', 'holiday_term', 'service_yn'], 1)
            insert_worksheet.insert_row(
                ['type', 'route_id', 'city', 'city_code', 'area', 'bus_no', 'bus_no_title', 'bus_no_sub_title',
                 'bus_company', 'start_point', 'end_point', 'turning', 'start_time', 'end_time', 'weekdays_term',
                 'weekend_term', 'holiday_term', 'service_yn'], 1)
        elif key == "정류장":
            update_worksheet.insert_row(
                ['STATION_NM', 'GPS_X', 'GPX_Y', 'MOBILE_NO', 'AREA', 'CITY_CODE', 'STATION_NM_PARSE', 'STAION_TYPE',
                 'DIRECTION', 'SERVICE_YN', 'API_ID', 'STATION_ID'], 1)
            insert_worksheet.insert_row(
                ['STATION_NM', 'GPS_X', 'GPX_Y', 'MOBILE_NO', 'AREA', 'CITY_CODE', 'STATION_NM_PARSE', 'STAION_TYPE',
                 'DIRECTION', 'SERVICE_YN', 'API_ID', 'STATION_ID'], 1)

        for i in update_data:
            indexNum = df.index[df[keyword] == i]
            # print(indexNum)
            # print(df.iloc[indexNum].values)
            update_worksheet.append_rows(df.iloc[indexNum].values.tolist())
            time.sleep(1)

        for i, j in delete_data:
            if key == "노선":
                delete_worksheet.append_row([i, j[0], j[1], j[2], j[3]], 1)
                time.sleep(1)
            elif key == "정류장":
                delete_worksheet.append_row([i, j], 1)
                time.sleep(1)

        for i in insert_data:
            indexNum = df.index[df[keyword] == i]
            insert_worksheet.append_rows(df.iloc[indexNum].values.tolist())
            time.sleep(1)


if __name__ == '__main__':
    ex = Merge()