import grpc

# import the generated classes
import dayhistory_pb2
import dayhistory_pb2_grpc

# open a gRPC channel
channel = grpc.insecure_channel('localhost:50051')

# create a stub (client)
stub = dayhistory_pb2_grpc.dayhistoryStub(channel)

# create a valid request message
ticker=input('Enter Ticker Here:- ')
number = dayhistory_pb2.Request(value=ticker)

# make the call
if(ticker=="all"):
        response = stub.GetAll(number)
else:
    response = stub.Get(number)

# print the results
print(response)



