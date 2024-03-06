import logging
import ssl
import sys
import os
from typing import Callable, Iterable, Sequence
import requests
import json
import random
import string
from methods import Method, QueryResponse, Status
from datetime import datetime
from time import sleep

unicode_replace = {u"\u2013": "-",u"\u2014": "-",u"\xab": '"',u"\xbb": '"',u"\xf6": 'o',u"\xca": 'e'}

def _unicode_filter(param):
    tmp = str(param)
    for u in unicode_replace:
        if tmp.find(u) != -1:
            tmp = tmp.replace(u, unicode_replace[u])
    return tmp

formatter = logging.Formatter(fmt = '%(asctime)s %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

log_info = setup_logger('info_log','yandex.log')
log_error = setup_logger('error_log','yandex.err')

host_market = "https://api.partner.market.yandex.ru"
host_delivery = "https://b2b-authproxy.taxi.yandex.net" #"https://b2b.taxi.tst.yandex.net"

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context



def post_query(url,json_body,headers) -> QueryResponse:
    try:
        r = requests.post(url, data = json.dumps(json_body),headers = headers)
    except Exception as ex:
        return QueryResponse(success=False, error_message='Request error. %s' % str(ex))

    if r.status_code != 200:
        return QueryResponse(success=False, status=r.status_code, data=r.text)

    try:
        response = json.loads(r.text)
    except Exception as ex:
        return QueryResponse(success=False, status=200, error_message='JSON error. %s' % str(ex))

    return QueryResponse(success=True, status=200, response=response)

def put_query(url,json_body,headers) -> QueryResponse:
    try:
        r = requests.put(url, data = json.dumps(json_body),headers = headers)
    except Exception as ex:
        return QueryResponse(success=False, error_message='Request error. %s' % str(ex))

    if r.status_code != 200:
        return QueryResponse(success=False, status=r.status_code, data=r.text)

    try:
        response = json.loads(r.text)
    except Exception as ex:
        return QueryResponse(success=False, status=200, error_message='JSON error. %s' % str(ex))

    return QueryResponse(success=True, status=200, response=response)

def get_query(url,headers) -> QueryResponse:
    try:
        r = requests.get(url, headers = headers)
    except Exception as ex:
        return QueryResponse(success=False, error_message='Request error. %s' % str(ex))

    if r.status_code != 200:
        return QueryResponse(success=False, status=r.status_code, data=r.text)

    try:
        response = json.loads(r.text)
    except Exception as ex:
        return QueryResponse(success=False, status=200, error_message='JSON error. %s' % str(ex))

    return QueryResponse(success=True, status=200, response=response)

def _post_without_xml(url:str, data: dict, headers) -> bool:
    body = data
    result = post_query(url, body, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error(('%s%s'),('Status: %s. ' % result.status) if result.status else '',result.error_message)
        return False

    return True

def _put_without_xml(url:str, data: dict, headers) -> bool:
    body = data
    result = put_query(url, body, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error(('%s%s'),('Status: %s. ' % result.status) if result.status else '',result.error_message)
        return False

    return True

def random_string(length: int) -> str:
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))


def _fill_section(xml_data: list, parent: Sequence, section: str, params: tuple, spacing: int=1):
    if isinstance(parent, dict):
        if section in parent:        
            xml_data.append('%s<%s>' % (''.join([' ']*spacing), section))
            if isinstance(parent[section], Iterable):
                for param in params:
                    if param in parent[section] and parent[section][param]:
                        xml_data.append('%s<%s>%s</%s>' % (''.join([' ']*(spacing+1)), param,_unicode_filter(parent[section][param]),param))
            xml_data.append('%s</%s>' % (''.join([' ']*spacing), section))
    elif isinstance(parent, list):
        for element in parent:
            xml_data.append('%s<%s>' % (''.join([' ']*spacing), section)) 
            for param in params:
                if param in element and element[param]:
                    xml_data.append('%s<%s>%s</%s>' % (''.join([' ']*(spacing+1)), param,_unicode_filter(element[param]),param))
            xml_data.append('%s</%s>' % (''.join([' ']*spacing), section))
    
def _order_to_xml(json_data, xml_data: list):
    xml_data.append(' <order>')
    for param in json_data.keys():
        if param.startswith('_'):
            xml_data.append('  <%s>%s</%s>' % (param, _unicode_filter(json_data[param]) ,param))    
    for param in ('id','status','substatus','creationDate','currency','itemsTotal','total','deliveryTotal','subsidyTotal','paymentType','paymentMethod','fake','notes','taxSystem'):
        if param in json_data:
            xml_data.append('  <%s>%s</%s>' % (param, _unicode_filter(json_data[param]), param))
    xml_data.append('  <delivery>')
    for param in ('id','price','deliveryPartnerType','deliveryServiceId','serviceName','type','dispatchType'):
        if param in json_data['delivery']:
            xml_data.append('   <%s>%s</%s>' % (param, _unicode_filter(json_data['delivery'][param]), param))
    xml_data.append('   <dates>')
    for param in ('fromDate','toDate','fromTime','toTime'):
        if param in json_data['delivery']['dates']:
            xml_data.append('    <%s>%s</%s>' % (param, _unicode_filter(json_data['delivery']['dates'][param]), param))
    xml_data.append('   </dates>')    
    xml_data.append('   <region>')
    for param in ('id','type','name'):
        if param in json_data['delivery']['region']:
            xml_data.append('    <%s>%s</%s>' % (param, _unicode_filter(json_data['delivery']['region'][param]), param))
    xml_data.append('   </region>')
    if 'address' in json_data['delivery']:
        xml_data.append('   <address>')
        for param in ('country','postcode','city','street','house','block','recipient'):
            if param in json_data['delivery']['address']:
                xml_data.append('    <%s>%s</%s>' % (param, _unicode_filter(json_data['delivery']['address'][param]), param))
        xml_data.append('   </address>')
    xml_data.append('   <shipments>')
    for shipment in json_data['delivery']['shipments']:
        xml_data.append('    <shipment>')
        for param in ('id','shipmentDate','shipmentTime','status','weight','width','height','depth'):
            if param in shipment:
                xml_data.append('     <%s>%s</%s>' % (param, _unicode_filter(shipment[param]), param))    
        if 'boxes' in shipment:
            xml_data.append('     <boxes>')
            for box in shipment['boxes']:
                xml_data.append('      <box>')
                for box_param in ('id','fulfilmentId','weight','width','height','depth'):
                    if box_param in box:
                        xml_data.append('       <%s>%s</%s>' % (box_param, _unicode_filter(box[box_param]), box_param))    
                xml_data.append('      </box>')
            xml_data.append('     </boxes>')
        xml_data.append('    </shipment>')
    xml_data.append('   </shipments>')
    xml_data.append('  </delivery>')
    if 'buyer' in json_data:
        xml_data.append('  <buyer>')
        for param in ('id','lastName','firstName','middleName','type'):
            if param in json_data['buyer']:
                xml_data.append('   <%s>%s</%s>' % (param, _unicode_filter(json_data['buyer'][param]), param))
        xml_data.append('  </buyer>')
    
    xml_data.append('  <items>')
    for item in json_data['items']:
        xml_data.append('   <item>')
        for param in ('id','offerId','count','price','vat','warehouseId','partnerWarehouseId','subsidy'):
            if param in item:
                xml_data.append('    <%s>%s</%s>' % (param, _unicode_filter(item[param]), param))    
        if 'promos' in item:
            xml_data.append('    <promos>')
            for promo in item['promos']:
                xml_data.append('     <promo>')
                for param in ('marketPromoId','subsidy','type'):
                    if param in promo:
                        xml_data.append('      <%s>%s</%s>' % (param, _unicode_filter(promo[param]), param))    
                xml_data.append('     </promo>')
            xml_data.append('    </promos>')
        if 'subsidies' in item:
            xml_data.append('    <subsidies>')
            for subsidy in item['subsidies']:
                xml_data.append('     <subsidy>')
                for param in ('amount','type'):
                    if param in subsidy:
                        xml_data.append('      <%s>%s</%s>' % (param, _unicode_filter(subsidy[param]), param))    
                xml_data.append('     </subsidy>')
            xml_data.append('    </subsidies>')
        xml_data.append('   </item>')
    xml_data.append('  </items>')
    xml_data.append(' </order>')


def json_to_xml(method: Method, json_data, xml_data: list):
    if method is Method.FBY_GET_ORDERS:
        for order in json_data:
            xml_data.append(' <fby_order>')
            for param in order.keys():
                if param.startswith('_'):
                    xml_data.append('  <param%s>%s</param%s>' % (param, _unicode_filter(order[param]) ,param))    
            for param in ('id','creationDate','status','statusUpdateDate','paymentType'):
                if param in order and order[param]:
                    xml_data.append('  <%s>%s</%s>' % (param, _unicode_filter(order[param]) ,param))
            xml_data.append('  <items>')
            for item in order['items']:
                xml_data.append('   <item>')
                for param in ('shopSku','marketSku','count'):
                    if param in item and item[param]:
                        xml_data.append('    <%s>%s</%s>' % (param, _unicode_filter(item[param]) ,param))
                xml_data.append('    <prices>')
                for price in item['prices']:
                    xml_data.append('     <price>')
                    for price_param in ('type','costPerItem','total'):
                        if price_param in price and price[price_param]:
                            xml_data.append('      <%s>%s</%s>' % (price_param, _unicode_filter(price[price_param]) ,price_param))
                    xml_data.append('     </price>')
                xml_data.append('    </prices>')
                xml_data.append('   </item>')
            xml_data.append('  </items>')
            xml_data.append(' </fby_order>')
    
    elif method is Method.FBS_SET_STATUS:
        _order_to_xml(json_data, xml_data)
    
    elif method is Method.FBS_SET_BOXES:
        xml_data.append('<order>')
        xml_data.append(' <id>%s</id>' % _unicode_filter(json_data['order']))
        xml_data.append(' <boxes>')
        for box in json_data['boxes']:
            xml_data.append('  <box>')
            for param in ('id','fulfilmentId','weight','width','height','depth'):
                if param in box:
                    xml_data.append('   <%s>%s</%s>' % (param, _unicode_filter(box[param]) ,param))
            xml_data.append('  </box>')
        xml_data.append(' </boxes>')
        xml_data.append('</order>')
    
    elif method is Method.GET_ORDER_INFO:
        _order_to_xml(json_data, xml_data)

    elif method is Method.GET_BUYER_INFO:
        xml_data.append('<buyer-info>')
        xml_data.append(' <order>%s</order>' % _unicode_filter(json_data['order']))
        for param in ('id','lastName','firstName','middleName','phone'):
            if param in json_data:
                xml_data.append(' <%s>%s</%s>' % (param, _unicode_filter(json_data[param]) ,param))
        xml_data.append('</buyer-info>')

    elif method is Method.SET_PRICES:
        xml_data.append('<set-prices>')
        for param in json_data.keys():
            if param.startswith('_'):
                xml_data.append('<%s>%s</%s>' % (param, _unicode_filter(json_data[param]) ,param))    
        xml_data.append(' <status>%s</status>' % json_data['status'])
        if 'errors' in json_data:
            xml_data.append(' <errors>')
            for error in json_data['errors']:
                xml_data.append('  <error>')
                for param in ('code','message'):
                    if param in error:
                        xml_data.append('   <%s>%s</%s>' % (param, _unicode_filter(error[param]) ,param))
                xml_data.append('  </error>')
            xml_data.append(' </errors>')
        
        xml_data.append('</set-prices>')

    elif method is Method.DBS_GET_ORDERS_FOR_CANCELLATION_APPROVE:
        for order in json_data:
            xml_data.append(' <dbs_order>')
            for param in ('id','status','substatus'):
                if param in order and order[param]:
                    xml_data.append('  <%s>%s</%s>' % (param, _unicode_filter(order[param]) ,param))
            xml_data.append(' </dbs_order>')
    
    elif method is Method.DBS_CHANGE_DATE:
        xml_data.append(' <dbs_change_date>')
        for param in json_data:
            xml_data.append('  <%s>%s</%s>' % (param, _unicode_filter(json_data[param]) ,param))
        xml_data.append(' </dbs_change_date>')
    
    elif method is Method.DBS_SET_STATUS:
        _order_to_xml(json_data, xml_data)
    
    elif method is Method.YD_CREATE_OFFER:
        xml_data.append('<yd-create-offer>')
        for param in json_data.keys():
            if param.startswith('_'):
                xml_data.append('<%s>%s</%s>' % (param, _unicode_filter(json_data[param]) ,param))    
        
        if 'error_details' in json_data:
            xml_data.append(' <errors>')
            for error in json_data['error_details']:
                xml_data.append('  <error>%s</error>' % _unicode_filter(error))
            xml_data.append(' </errors>')
        else:
            for param in ('operator_request_id','pickup_interval_min','pickup_interval_max','request_id'):
                if param in json_data:
                    xml_data.append(' <%s>%s</%s>' % (param, _unicode_filter(json_data[param]), param))
        
        xml_data.append('</yd-create-offer>')

    elif method is Method.YD_CANCEL_OFFER:
        xml_data.append('<yd-cancel-offer>')
        xml_data.append(' <request_id>%s</request_id>' % _unicode_filter(json_data['request_id']))
        xml_data.append(' <timestamp>%s</timestamp>' % datetime.strftime(datetime.now(), "%d-%m-%Y %H:%M:%S"))
        xml_data.append('</yd-cancel-offer>')

    elif method is Method.YD_OFFER_INFO:
        xml_data.append('<yd-offer-info>')
        xml_data.append(' <request_id>%s</request_id>' % _unicode_filter(json_data['request_id']))
        for param in ('timestamp','status','sharing_url','total_without_vat'):
            if param in json_data:
                xml_data.append(' <%s>%s</%s>' % (param, _unicode_filter(json_data[param]) ,param))
        
        xml_data.append('</yd-offer-info>')


def fby_get_orders(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    xml.append('<?xml version="1.0" encoding="windows-1251"?>')
    xml.append('<fby_orders>')
    body = data
    strings_exist = False
    only_statuses = ('DELIVERED','RETURNED','REJECTED','PICKUP','DELIVERY','CANCELLED_IN_PROCESSING')
    page_token = None
    while True:
        if page_token:
            url = f"{host_market}/v2/campaigns/{campaign_id}/stats/orders.json?limit=200&page_token={page_token}"    
        else:
            url = f"{host_market}/v2/campaigns/{campaign_id}/stats/orders.json?limit=200"
        result = post_query(url, body, headers)
        if not result.success:
            if result.data:
                log_error.error('Status: %s. Error: %s',result.status,result.data)
            else:
                log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
            return False

        log_info.info('URL %s, body %s, status %s, response status %s', url, str(body), result.status, result.response['status'] if 'status' in result.response else '')
        if 'result' in result.response and 'orders' in result.response['result']:
            log_info.info('orders %s', len(result.response['result']['orders']))
        
        try:
            if 'result' in result.response:
                orders = [order for order in result.response['result']['orders'] if 'status' in order and order['status'] in only_statuses]
            else:
                orders = []

            if 'params' in data:
                for param in dict(data['params']).keys():                    
                    for order in orders:
                        order['_%s' % str(param)] = data['params'][param]                

            if orders:
                json_to_xml(Method.FBY_GET_ORDERS, orders, xml)
                strings_exist = True
        except Exception as ex:
            log_error.error('%s', str(ex))
            return False

        page_token = None
        if 'paging' in result.response['result'] and 'nextPageToken' in result.response['result']['paging']:
            page_token = result.response['result']['paging']['nextPageToken']
        else:
            break

    xml.append('</fby_orders>')

    return strings_exist

def fbs_set_status(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/v2/campaigns/{campaign_id}/orders/{data['order']}/status.json"
    
    if data['status'] == Status.SET_READY.value:
        body = {'order': {'status': 'PROCESSING', 'substatus': 'READY_TO_SHIP'}}
    elif data['status'] == Status.CANCELLED.value:
        body = {'order': {'status': 'CANCELLED', 'substatus': data['substatus']}}
    else:
        body = {}

    result = put_query(url, body, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, body %s, status %s', url, str(body), result.status)
    log_info.info('Response %s', result.response)

    order = result.response['order'] if 'order' in result.response else {}
    
    try:
        if 'params' in data:
            for param in dict(data['params']).keys():                    
                order['_%s' % str(param)] = data['params'][param]                
        if order:
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            json_to_xml(Method.FBS_SET_STATUS, order, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def fbs_set_boxes(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/v2/campaigns/{campaign_id}/orders/{data['order']}/delivery/shipments/{data['shipment']}/boxes.json"
    
    if 'boxes' in data:
        body = {'boxes': data['boxes']}
    else:
        body = {}

    result = put_query(url, body, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, body %s, status %s, response status %s', url, str(body), result.status, result.response['status'] if 'status' in result.response else '')
    log_info.info('Response %s', result.response)
    if 'result' in result.response and 'boxes' in result.response['result']:
        log_info.info('boxes %s', len(result.response['result']['boxes']))
        boxes = result.response['result']['boxes']
    else:
        boxes = []
    
    try:
        if boxes:
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            json_to_xml(Method.FBS_SET_BOXES, {'order': data['order'], 'boxes': boxes}, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def get_order_info(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/v2/campaigns/{campaign_id}/orders/{data['order']}.json"
    
    result = get_query(url, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, status %s, response status %s', url, result.status, result.response['status'] if 'status' in result.response else '')
    log_info.info('Response %s', result.response)
    
    order = result.response['order'] if 'order' in result.response else {}
    
    if 'params' in data:
        for param in dict(data['params']).keys():                    
            order['_%s' % str(param)] = data['params'][param]                

    try:
        if order:
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            json_to_xml(Method.GET_ORDER_INFO, order, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def get_buyer_info(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/v2/campaigns/{campaign_id}/orders/{data['order']}/buyer.json"
    
    result = get_query(url, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, status %s, response status %s', url, result.status, result.response['status'] if 'status' in result.response else '')
    log_info.info('Response %s', result.response)
    
    buyer = result.response['result'] if 'result' in result.response else {}

    try:
        if buyer:
            buyer['order'] = data['order']
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            json_to_xml(Method.GET_BUYER_INFO, buyer, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def set_prices(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/v2/campaigns/{campaign_id}/offer-prices/updates.json"
    
    if 'prices' in data:
        body = {'offers': data['prices']}
    else:
        body = {}

    result = post_query(url, body, headers)
    if not result.success:
        try:
            error_data = json.loads(result.data)
        except Exception as ex:
            error_data = {}
        
        if result.status == 400 and 'status' in error_data:
            try:
                if 'params' in data:
                    _add_params_to_dict(data['params'], error_data)
                xml.append('<?xml version="1.0" encoding="windows-1251"?>')
                json_to_xml(Method.SET_PRICES, error_data, xml)
                strings_exist = True
                return True
            except Exception as ex:
                log_error.error('%s', str(ex))
                return False
        elif result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('Response %s', result.response)

    response_data = dict(result.response)
    try:
        if 'params' in data:
            _add_params_to_dict(data['params'], response_data)        
        xml.append('<?xml version="1.0" encoding="windows-1251"?>')
        json_to_xml(Method.SET_PRICES, response_data, xml)
        strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist


def dbs_get_orders_for_cancellation_approve(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    xml.append('<?xml version="1.0" encoding="windows-1251"?>')
    xml.append('<dbs_orders_for_cancellation_approve>')
    strings_exist = False
    url = f"{host_market}/v2/campaigns/{campaign_id}/orders?onlyWaitingForCancellationApprove=true"
    result = get_query(url, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, status %s, response status %s', url, result.status, result.response['status'] if 'status' in result.response else '')
    if 'orders' in result.response:
        log_info.info('orders %s', len(result.response['orders']))
        orders = result.response['orders']
    else:
        orders = []
    
    try:
        if orders:
            json_to_xml(Method.DBS_GET_ORDERS_FOR_CANCELLATION_APPROVE, orders, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    xml.append('</dbs_orders_for_cancellation_approve>')

    return strings_exist

def dbs_cancellation_accept(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    body = {'accepted': data['accepted']}
    if 'reason' in data:
        body['reason'] = data['reason']
    return _put_without_xml(f"{host_market}/v2/campaigns/{campaign_id}/orders/{data['order']}/cancellation/accept", body, {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'})

def dbs_change_date(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/campaigns/{campaign_id}/orders/{data['order_id']}/delivery/date"
    result = put_query(url, data, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, body %s, status %s', url, str(data), result.status)
    log_info.info('Response %s', result.response)

    if 'status' not in result.response or result.response['status']!='OK':
        return False
    
    order = {'order': data['order_id']}
    if 'dates' in data and 'toDate' in data['dates']:
        order['date'] = data['dates']['todate']

    try:
        if 'params' in data:
            for param in dict(data['params']).keys():                    
                order['_%s' % str(param)] = data['params'][param]                
        xml.append('<?xml version="1.0" encoding="windows-1251"?>')
        json_to_xml(Method.DBS_CHANGE_DATE, order, xml)
        strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def dbs_set_track(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    return _post_without_xml(f"{host_market}/campaigns/{campaign_id}/orders/{data['order']}/delivery/track", {'trackCode': data['track'], 'deliveryServiceId': data['deliveryId']}, {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'})

def dbs_set_status(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'}
    strings_exist = False
    url = f"{host_market}/campaigns/{campaign_id}/orders/{data['order_id']}/status"
    result = put_query(url, data, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, body %s, status %s', url, str(data), result.status)
    log_info.info('Response %s', result.response)

    order = result.response['order'] if 'order' in result.response else {}
    
    try:
        if 'params' in data:
            for param in dict(data['params']).keys():                    
                order['_%s' % str(param)] = data['params'][param]                
        if order:
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            json_to_xml(Method.DBS_SET_STATUS, order, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def yd_create_offer(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'Bearer {token}'}
    strings_exist = False
    url = f"{host_delivery}/api/b2b/platform/offers/create"
    
    result = post_query(url, data, headers)
    if not result.success:
        try:
            error_data = json.loads(result.data)
        except Exception as ex:
            error_data = {}
        
        if result.status == 400 and 'message' in error_data:
            try:
                xml.append('<?xml version="1.0" encoding="windows-1251"?>')
                error_data['operator_request_id'] = data['info']['operator_request_id']
                json_to_xml(Method.YD_CREATE_OFFER, error_data, xml)
                strings_exist = True
                return True
            except Exception as ex:
                log_error.error('%s', str(ex))
                return False
        elif result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    #все норм и предложены варианты
    
    log_info.info('Response %s', result.response)

    response_data = dict(result.response)

    latest_date = 0
    delivery_to_time_interval = False
    result_offer = {}    
    for offer in response_data['offers']:
        max_date = datetime.fromisoformat(offer['offer_details']['pickup_interval']['max'][:19]).timestamp()

        if delivery_to_time_interval:
            if offer['offer_details']['delivery_interval']['policy'] == "time_interval":
                if max_date > latest_date:
                    latest_date = max_date
                    result_offer = dict(offer)
                continue
        
        if offer['offer_details']['delivery_interval']['policy'] == "time_interval":
            latest_date = max_date
            result_offer = dict(offer)
            delivery_to_time_interval = True
        else:
            if max_date > latest_date:
                latest_date = max_date
                result_offer = dict(offer)
            delivery_to_time_interval = False


    if result_offer:
        url = f"{host_delivery}/api/b2b/platform/offers/confirm"
        
        body = {'offer_id': result_offer['offer_id']}

        result = post_query(url, body, headers)
        if not result.success:
            try:
                error_data = json.loads(result.data)
            except Exception as ex:
                error_data = {}
            
            if (result.status == 404 or result.status == 500) and 'message' in error_data:
                try:
                    xml.append('<?xml version="1.0" encoding="windows-1251"?>')
                    error_data['operator_request_id'] = data['info']['operator_request_id']
                    json_to_xml(Method.YD_CREATE_OFFER, error_data, xml)
                    strings_exist = True
                    return True
                except Exception as ex:
                    log_error.error('%s', str(ex))
                    return False
            elif result.data:
                log_error.error('Status: %s. Error: %s',result.status,result.data)
            else:
                log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
            return False

    response_data = dict(result.response)
    try:
        if 'params' in data:
            _add_params_to_dict(data['params'], response_data)        
        xml.append('<?xml version="1.0" encoding="windows-1251"?>')
        response_data['operator_request_id'] = data['info']['operator_request_id']
        response_data['pickup_interval_min'] = result_offer['offer_details']['pickup_interval']['min'][:19]
        response_data['pickup_interval_max'] = result_offer['offer_details']['pickup_interval']['max'][:19]
        json_to_xml(Method.YD_CREATE_OFFER, response_data, xml)
        strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist

def yd_cancel_offer(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    if _post_without_xml(f"{host_delivery}/api/b2b/platform/request/cancel", data, {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'Bearer {token}'}):
        try:
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            response_data = {'request_id': data['request_id']}
            json_to_xml(Method.YD_CANCEL_OFFER, response_data, xml)
            return True
        except Exception as ex:
            log_error.error('%s', str(ex))
            return False
    return False
    
def yd_offer_info(client_id: str, token: str, campaign_id: str, xml: list, data) -> bool:
    headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': f'Bearer {token}'}
    strings_exist = False
    url = f"{host_delivery}/api/b2b/platform/request/info?request_id={data['request_id']}"
    
    result = get_query(url, headers)
    if not result.success:
        if result.data:
            log_error.error('Status: %s. Error: %s',result.status,result.data)
        else:
            log_error.error('%s%s',('Status: ' + result.status +'. ') if result.status else '',result.error_message)
        return False

    log_info.info('URL %s, status %s, response status %s', url, result.status, result.response['status'] if 'status' in result.response else '')
    log_info.info('Response %s', result.response)

    state = result.response['state'] if 'state' in result.response else {}

    param = 'total_without_vat'
    if 'pricing' in result.response and 'price' in result.response['pricing'] and param in result.response['pricing']['price']:
        state[param] = result.response['pricing']['price'][param]

    try:
        if state:
            state['request_id'] = data['request_id']
            if 'sharing_url' in result.response:
                state['sharing_url'] = result.response['sharing_url']
            xml.append('<?xml version="1.0" encoding="windows-1251"?>')
            json_to_xml(Method.YD_OFFER_INFO, state, xml)
            strings_exist = True
    except Exception as ex:
        log_error.error('%s', str(ex))
        return False

    return strings_exist


def _add_params_to_dict(params: dict, destination: dict):
    for param in params.keys():                    
        destination['_%s' % str(param)] = params[param]                


functions = {
    Method.FBY_GET_ORDERS: (fby_get_orders, 'yafby_'),
    Method.FBS_SET_STATUS: (fbs_set_status, 'ymt_'),
    Method.FBS_SET_BOXES: (fbs_set_boxes, 'ymb_'),
    Method.GET_ORDER_INFO: (get_order_info, 'ymt_'),
    Method.GET_BUYER_INFO: (get_buyer_info, 'ymbi_'),
    Method.SET_PRICES: (set_prices, 'ypr_'),
    Method.DBS_GET_ORDERS_FOR_CANCELLATION_APPROVE: (dbs_get_orders_for_cancellation_approve, 'yadbs_'),
    Method.DBS_CANCELLATION_ACCEPT: (dbs_cancellation_accept, ''),
    Method.DBS_SET_TRACK: (dbs_set_track, ''),
    Method.DBS_SET_STATUS: (dbs_set_status, 'ymt_'),
    Method.DBS_CHANGE_DATE: (dbs_change_date, 'yachd_'),
    Method.YD_CREATE_OFFER: (yd_create_offer, 'ydcr_'),
    Method.YD_CANCEL_OFFER: (yd_cancel_offer, 'ydca_'),
    Method.YD_OFFER_INFO: (yd_offer_info, 'ydgi_')
    }

def _execute_method(client_id: str, token: str, campaign_id: str, func_method: Callable, xml_prefix: str, func_data: dict) -> bool:
    response_xml = []
    write_xml = False

    try:
        write_xml = func_method(client_id, token, campaign_id, response_xml, func_data)
    except Exception as e:
        log_error.error('Method error: %s', str(e))
        return False

    if write_xml:
        if xml_prefix:
            out_file = os.path.join(file_path, xml_prefix + random_string(8-len(xml_prefix)) + '.xml')
            log_info.info('Out file: %s', out_file)
            try:
                with open(out_file, "wb") as result_xml:
                    result_xml.write('\n'.join(response_xml).encode('cp1251', errors='ignore'))
            except Exception as e:
                log_error.error('Write error: %s', str(e))
                return False
    return True


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        log_error.error('Illegal arguments count!')
        sys.exit(10)

    function_data = []
    not_delete = False
    delete_anyway = False
    delete_before_execution = False

    file_path = sys.argv[2] if len(sys.argv) == 3 else os.path.dirname(sys.argv[1])
    
    try:
        try:
            config = json.load(open(sys.argv[1], 'r', encoding='utf-8'))
        except Exception:
            config = json.load(open(sys.argv[1], 'r', encoding='cp1251'))
            
        CLIENT_ID = config['client_id']
        TOKEN = config['token']
        CAMPAIGN_ID = config['campaign_id']
        not_delete = config['not_delete'] if 'not_delete' in config else False
        delete_anyway = config['delete_anyway'] if 'delete_anyway' in config else False
        delete_before_execution = config['delete_before_execution'] if 'delete_before_execution' in config else False

        if 'xml_path' in config and config['xml_path']:
            file_path = config['xml_path']

        execute_requests = []
        if not isinstance(config['request'], list):
            execute_requests.append(config['request'])
        else:
            execute_requests = config['request']
    except Exception as e:
        log_error.error('Read config error: ' + str(e))
        sys.exit(20)

    if delete_before_execution:
        if os.path.exists(sys.argv[1]):
            os.remove(sys.argv[1])

    execute_result = True
    is_not_required = False
    pause_before = 0

    for single_request in execute_requests:
        if not execute_result and not is_not_required: #цепочка запросов до 1-й ошибки в обязательном запросе
            break
        method = single_request['method']
        is_not_required = True if 'is_not_required' in single_request and single_request['is_not_required']==1 else False
        pause_before = int(single_request['pause_before']) if 'pause_before' in single_request and str(single_request['pause_before']).isdigit() else 0
        function_data = single_request['data'] if 'data' in single_request else []
        
        if pause_before > 0:
            if pause_before > 180:
                pause_before = 180
            print(f"Pause {pause_before} seconds")
            sleep(pause_before)
        execute_result =_execute_method(CLIENT_ID, TOKEN, CAMPAIGN_ID, *functions[Method(method)], function_data)

    if not delete_before_execution and delete_anyway:
        if os.path.exists(sys.argv[1]):
            os.remove(sys.argv[1])

    if not execute_result:
        sys.exit(30)

    if not delete_before_execution and not delete_anyway and not not_delete: #если уже точно не удалено, то удалять, если нет особых указаний
        if os.path.exists(sys.argv[1]):
            os.remove(sys.argv[1])


