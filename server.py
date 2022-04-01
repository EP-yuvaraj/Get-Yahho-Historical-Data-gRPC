from urllib import response
import grpc
from concurrent import futures
import time
import random
import datetime
# import the generated classes
import dayhistory_pb2
import dayhistory_pb2_grpc

# import the original calculator.py
import dayhistory
# create a class to define the server functions, derived from
# calculator_pb2_grpc.CalculatorServicer
class dayhistoryServicer(dayhistory_pb2_grpc.dayhistoryServicer):


    # calculator.square_root is exposed here
    # the request and response are of the data type
    # calculator_pb2.Number
    def GetAll(self, request, context):
        db_data = dayhistory.get_data(request.value)
        total=len(db_data)
        new_data=[]
        for i in range(0,total):
        # stock_detail_id=request.stock_detail_id
            currernt_price_date = db_data[i][1].strftime("%H:%M:%S.%f - %b %d %Y")
            created_by = db_data[i][13].strftime("%H:%M:%S.%f - %b %d %Y")
            last_modified_by = db_data[i][15].strftime("%H:%M:%S.%f - %b %d %Y")

            data=dayhistory_pb2.TickerResponse(
                stock_price_historical_id=db_data[i][0],
                historical_price_date_time=currernt_price_date,
                day_open_price=db_data[i][2],
                day_high_price=db_data[i][3],
                day_low_price=db_data[i][4],
                day_close_price=db_data[i][5],
                day_volume=db_data[i][6],
                ticker_id=db_data[i][7],
                bu_id=db_data[i][8],
                sub_bu_id=db_data[i][9],
                application_id=db_data[i][10],
                is_active=db_data[i][11],
                created_by=db_data[i][12],
                created_date=created_by,
                last_modified_by=db_data[i][14],
                last_modified_date=last_modified_by,
                isin_code=db_data[i][16]
            )
            new_data.append(data)
        # books_to_recommend = random.sample(
        #     new_data,total
        # )
        print(new_data)

            
            # response = calculator_pb2.Number()
            # response.value = calculator.square_root(request.value)
        return dayhistory_pb2.TickerResponses(response=new_data)

    def Get(self, request, context):
        db_data = dayhistory.get_data(request.value)
        total=len(db_data)
        new_data=[]
        # stock_detail_id=request.stock_detail_id
        currernt_price_date = db_data[1].strftime("%H:%M:%S.%f - %b %d %Y")
        created_by = db_data[13].strftime("%H:%M:%S.%f - %b %d %Y")
        last_modified_by = db_data[15].strftime("%H:%M:%S.%f - %b %d %Y")

        data=dayhistory_pb2.TickerResponse(
            stock_price_historical_id=db_data[0],
            historical_price_date_time=currernt_price_date,
            day_open_price=db_data[2],
            day_high_price=db_data[3],
            day_low_price=db_data[4],
            day_close_price=db_data[5],
            day_volume=db_data[6],
            ticker_id=db_data[7],
            bu_id=db_data[8],
            sub_bu_id=db_data[9],
            application_id=db_data[10],
            is_active=db_data[11],
            created_by=db_data[12],
            created_date=created_by,
            last_modified_by=db_data[14],
            last_modified_date=last_modified_by,
            isin_code=db_data[16]
            )
        new_data.append(data)
        # books_to_recommend = random.sample(
        #     new_data,total
        # )
        print(new_data)

            
            # response = calculator_pb2.Number()
            # response.value = calculator.square_root(request.value)
        return dayhistory_pb2.TickerResponses(response=new_data)



# create a gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# use the generated function `add_CalculatorServicer_to_server`
# to add the defined class to the server
dayhistory_pb2_grpc.add_dayhistoryServicer_to_server(
        dayhistoryServicer(), server)

# listen on port 50051
print('Starting server. Listening on port 50051.')
server.add_insecure_port('[::]:50051')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)




