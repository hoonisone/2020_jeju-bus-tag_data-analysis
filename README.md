# jeju-bus-stations-clustering
제주 버스 정류장 군집화 알고리즘s

# flow
1. 관광객 아이디 추출
 * input - bus_tag_data.csv 여러개
 * process - 관광객 추출 알고리즘
 * ouput - user_list.csv

2. 정류장 컬럼 생성
* input - bus_tag_data 여러개, user_list.csv
* process - 관광객 컬럼 생성 => bus_tag_data 에 관광객 여부를 모두 표시해준다.
* output - bus_tag_data_with_tourist_column.csv 여러개 => 기존 bus_tag_data  파일을 수정함

3. 정류장 추출
* input - bus_tag_data_with_tourist_column.csv 
* process - 정류장 추출 with 주소 컬럼
* output - usage_analysis_data.csv

4. 정류장 군집화
* input - usage_analysis_data.csv
* process - 군집화 알고리즘
* ouput - usage_analysis_data_with_cluster_column.csv => 기존 usage_analysis_data 수정

5. 정류장 군집 시각화
* input - usage_analysis_data_with_cluster_column.csv
* process - 정류장 군집 시각화

6. 군집별 이용량 시각화
* input - usage_analysis_data_with_cluster_column.csv &  bus_tag_data.csv 여러개 
* process - 
7. 군집별 체류시간 시각화

8. 군집별 이동패턴 분석
* Trie 구조를 활용한 군집별 이동패턴 저장 및 탐색 알고리즘 구현
* 군집별 이동패턴 시각화

# problem
[A 문제 상황]
1. 데이터 호환성 부족
  1) 데이터 종류
    * 정류장 데이터
    * 태그 데이터

  2) 호환성 부족 
    * 정류장 id 호환 안됨
    * 위도, 경도 값 조금씩 상이
      - 양쪽 데이터 모두 오차가 존재
      - 바다 근처의 경우 위치-주소 변환 시 위치가 바다로 되어있는 경우 오류 발생
      
  3) 해결방안
    * 정류장 데이터를 사용하지 않는다.
    * 분석 기간을 설정하고 해당 기간 동안 사용된 정류장을 가지고 태그 데이터를 분석한다.
      - 일정 기간내 다음 스키마를 같는 데이터 정리 필요
        -> ["id", "name", "latitude", "logitude", "address"]

  4) 추가
    * 입력 기간동안 사용되는 정류장을 모두 고려하기 위해 전체 데이터 탐색이 요구됨
      - 동시에 사용량을 정리하면 탐색 횟수를 줄일 수 있다.
      - 각 정류장 별 사용량을 구하기 위해서도 전체 이용데이터를 처리해야 하기 때문
