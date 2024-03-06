from typing import NamedTuple
from enum import Enum

class QueryResponse(NamedTuple):
    success: bool = False
    status: str = ''
    error_message: str = ''
    data: str = ''
    response: dict = {}

class Method(Enum):
    FBY_GET_ORDERS = 'fby_get_orders'
    FBS_SET_STATUS = 'fbs_set_status'
    FBS_SET_BOXES = 'fbs_set_boxes'
    GET_ORDER_INFO = 'get_order_info'
    GET_BUYER_INFO = 'get_buyer_info'
    SET_PRICES = 'set_prices'
    DBS_GET_ORDERS_FOR_CANCELLATION_APPROVE = 'dbs_get_orders_for_cancellation_approve'
    DBS_CANCELLATION_ACCEPT = 'dbs_cancellation_accept'
    DBS_SET_TRACK = 'dbs_set_track'
    DBS_SET_STATUS = 'dbs_set_status'
    DBS_CHANGE_DATE = 'dbs_change_date'
    YD_CREATE_OFFER = 'yd_create_offer'
    YD_CANCEL_OFFER = 'yd_cancel_offer'
    YD_OFFER_INFO = 'yd_offer_info'

class Status(Enum):
    SET_READY = 'SET_READY'
    CANCELLED = 'CANCELLED'
    

