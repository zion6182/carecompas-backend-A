"""Shared DB Lambda transport, authenticated capabilities, and DynamoDB helpers."""
from __future__ import annotations
import base64
import hashlib
import hmac
import json
import logging
import math
import os
import re
import time
import uuid
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

API_VERSION = '2'
REGION = 'us-east-1'
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'carecompass-data-<ACCOUNT_ID>-us-east-1')
TABLE_NAMES = {name: 'carecompass-'+name for name in ('UserPatientLinks','Records','FollowUps','Clinics')}
CURSOR_AUD = 'carecompass-db-cursor-v1'
CURSOR_SECONDS = 900
_ddb_resource = None
_ddb_lowlevel = None
_secret_cache = None
_secret_cached_at = 0
logger = logging.getLogger('carecompass.db')
logger.setLevel(logging.INFO)
_audit_context = ContextVar('carecompass_audit', default=None)
_auth_context = ContextVar('carecompass_auth', default=None)

def _audit_patient(claims):
    audit = _audit_context.get()
    if audit is not None:
        audit.update(principalHash=claims['principalHash'], patientKey=claims['patientKey'],
                     datasetId=claims['datasetId'])

def authenticate_request(event):
    """Only a verified Cognito token may establish the request's caller identity."""
    from cognito_auth import AuthError, verify_access_token
    audit = _audit_context.get()
    auth = event.get('auth')
    try:
        if auth is None:
            raise AuthError('AUTH_REQUIRED', 'auth.accessToken is required')
        if not isinstance(auth, dict) or set(auth) != {'accessToken'}:
            raise AuthError('INVALID_ACCESS_TOKEN', 'auth must contain only accessToken')
        verified = verify_access_token(auth['accessToken'])
    except AuthError as exc:
        if audit is not None:
            audit.update(authOutcome='ERROR' if exc.retryable or exc.code in {'CONFIGURATION_ERROR', 'AUTH_UNAVAILABLE'} else 'DENIED',
                         authErrorCode=exc.code)
        raise ApiError(exc.code, exc.message, exc.retryable) from None
    except Exception:
        if audit is not None:
            audit.update(authOutcome='ERROR', authErrorCode='AUTH_UNAVAILABLE')
        raise ApiError('AUTH_UNAVAILABLE', 'Authentication service is temporarily unavailable', True) from None
    claims = verified['claims']
    principal = binding_hash({'accountIssuer': claims['iss'], 'accountSubject': claims['sub']})
    _auth_context.set({'issuer': claims['iss'], 'subject': claims['sub'], 'principalHash': principal})
    if audit is not None:
        audit.update(identitySource='VERIFIED_COGNITO_ACCESS_TOKEN', principalHash=principal,
                     authOutcome='SUCCESS', revocationChecked=True,
                     tokenIssuedAt=claims['iat'], tokenExpiresAt=claims['exp'],
                     cognitoRequestId=verified['cognitoRequestId'])
        for claim, field in [('jti', 'tokenIdHash'), ('origin_jti', 'sessionHash')]:
            if isinstance(claims.get(claim), str):
                audit[field] = binding_hash({'issuer': claims['iss'], 'id': claims[claim]})
        if type(claims.get('auth_time')) is int:
            audit['authTime'] = claims['auth_time']

def authenticated_account():
    identity = _auth_context.get()
    if identity is None:
        raise ApiError('AUTH_REQUIRED', 'A verified Cognito access token is required')
    return identity

def resolve_patient_scope(required=True):
    """Resolve ownership from a strongly consistent server-side account link each request."""
    identity = authenticated_account()
    if 'resolution' not in identity:
        pk = account_key(identity['issuer'], identity['subject'])
        row = get_item('UserPatientLinks', pk, 'LINK')
        if row is None:
            resolution = {'linked': False, 'reason': 'NOT_LINKED'}
        else:
            if (row.get('PK') != pk or row.get('SK') != 'LINK'
                    or row.get('accountIssuer') != identity['issuer']
                    or row.get('accountSubject') != identity['subject']):
                raise ApiError('DATA_INTEGRITY_ERROR', 'Stored account link identity is invalid')
            if row.get('linkStatus') != 'ACTIVE':
                resolution = {'linked': False, 'reason': 'INACTIVE'}
            else:
                try:
                    scope = _patient_claims({key: row.get(key) for key in
                        ['patientKey', 'datasetId', 'sourceSystems', 'patientResourceKey', 'isSynthetic']}
                        | {'principalHash': identity['principalHash']})
                except ApiError:
                    raise ApiError('DATA_INTEGRITY_ERROR', 'Stored account link scope is invalid') from None
                scope.update(accountIssuer=identity['issuer'], accountSubject=identity['subject'])
                resolution = {'linked': True, 'scope': scope}
        identity['resolution'] = resolution
    resolution = identity['resolution']
    audit = _audit_context.get()
    if audit is not None:
        audit['accountLinked'] = resolution['linked']
        if not resolution['linked']: audit['linkReason'] = resolution['reason']
    if not resolution['linked']:
        if required:
            raise ApiError('PATIENT_NOT_LINKED' if resolution['reason'] == 'NOT_LINKED' else 'PATIENT_LINK_INACTIVE',
                           'An active patient account link is required')
        return dict(resolution)
    _audit_patient(resolution['scope'])
    return dict(resolution['scope'])

def require_patient_scope():
    return resolve_patient_scope(required=True)

def _audit_targets(params):
    audit = _audit_context.get()
    if audit is None: return
    keys = []
    value = params.get('resourceKey')
    if isinstance(value, str) and len(value) <= 1024: keys.append(value)
    for field, key_field in [('records', 'resourceKey'), ('followups', 'sourceResourceKey')]:
        values = params.get(field)
        if isinstance(values, list):
            audit['requested' + field.capitalize() + 'Count'] = len(values)
            for item in values[:50]:
                value = item.get(key_field) if isinstance(item, dict) else None
                if isinstance(value, str) and len(value) <= 1024: keys.append(value)
    if keys: audit['requestedResourceKeyHashes'] = sorted({sha256(value) for value in keys})[:100]

def _audit_result(data):
    audit = _audit_context.get()
    if audit is None or not isinstance(data, dict): return
    if isinstance(data.get('items'), list): audit['returnedItemCount'] = len(data['items'])
    if type(data.get('linked')) is bool:
        audit['accountLinked'] = data['linked']
        if data.get('reason') in {'NOT_LINKED', 'INACTIVE'}: audit['linkReason'] = data['reason']
    if type(data.get('idempotentReplay')) is bool: audit['idempotentReplay'] = data['idempotentReplay']
    if type(data.get('stateVersion')) is int: audit['stateVersion'] = data['stateVersion']
    for field in ['records', 'followups']:
        if isinstance(data.get(field), dict):
            audit[field + 'Result'] = {key: value for key, value in data[field].items()
                                      if key in {'accepted', 'created', 'updated', 'unchanged', 'recovered'} and type(value) is int}

def _emit_audit(audit):
    # Logging must not turn an already committed write into a reported business failure.
    try: logger.info('db_access', extra={'audit': audit})
    except Exception:
        try: logger.error('audit_emit_failed')
        except Exception: pass

class ApiError(Exception):
    def __init__(self, code, message, retryable=False, details=None):
        super().__init__(message)
        self.code, self.message, self.retryable, self.details = code, message, retryable, details

def require_dict(value, name='value'):
    if not isinstance(value,dict): raise ApiError('INVALID_REQUEST',f'{name} must be an object')
    return value

def validate_identifier(value, name='identifier', max_length=128):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,'+str(max_length-1)+r'}',value):
        raise ApiError('INVALID_REQUEST',f'{name} is invalid')
    return value

def native_json(value):
    if isinstance(value,Decimal):
        if not value.is_finite():raise ApiError('INVALID_DATA','Non-finite numeric value')
        return int(value) if value==value.to_integral_value() else float(value)
    if isinstance(value,dict):return {k:native_json(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [native_json(v) for v in value]
    if isinstance(value,float) and not math.isfinite(value):raise ApiError('INVALID_DATA','Non-finite numeric value')
    return value

def canonical_json(value):
    return json.dumps(native_json(value),ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def sha256(value):
    return hashlib.sha256(value.encode('utf-8') if isinstance(value,str) else value).hexdigest()

def canonicalhash(value):return sha256(canonical_json(value))
def now_iso():return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')

def account_key(issuer,subject):
    for name,value,limit in [('accountIssuer',issuer,2048),('accountSubject',subject,256)]:
        if not isinstance(value,str) or not value or len(value)>limit or any(ord(c)<32 for c in value):
            raise ApiError('INVALID_REQUEST',name+' is invalid')
    return 'ACCOUNT#'+sha256(issuer+'\n'+subject)

def _aws():
    if os.environ.get('AWS_REGION',REGION)!=REGION:raise ApiError('CONFIGURATION_ERROR','Only us-east-1 is supported')
    import boto3
    return boto3

def table(name):
    global _ddb_resource
    if name not in TABLE_NAMES:raise ApiError('INVALID_REQUEST','Unsupported table')
    if _ddb_resource is None:_ddb_resource=_aws().resource('dynamodb',region_name=REGION)
    return _ddb_resource.Table(TABLE_NAMES[name])

def ddb_client():
    global _ddb_lowlevel
    if _ddb_lowlevel is None:_ddb_lowlevel=_aws().client('dynamodb',region_name=REGION)
    return _ddb_lowlevel

def _ddb_value(value):
    if isinstance(value,float):
        if not math.isfinite(value):raise ApiError('INVALID_DATA','Non-finite numeric value')
        return Decimal(str(value))
    if isinstance(value,dict):return {k:_ddb_value(v) for k,v in value.items()}
    if isinstance(value,list):return [_ddb_value(v) for v in value]
    return value

def typed_item(value):
    from boto3.dynamodb.types import TypeSerializer
    serializer=TypeSerializer()
    return {k:serializer.serialize(_ddb_value(v)) for k,v in value.items()}

def get_item(name,pk,sk):
    return table(name).get_item(Key={'PK':pk,'SK':sk},ConsistentRead=True).get('Item')

def batch_get(name,keys):
    if not keys:return []
    table(name)
    unique=list({(x['PK'],x['SK']):{'PK':x['PK'],'SK':x['SK']} for x in keys}.values())
    output=[]
    for start in range(0,len(unique),100):
        pending={TABLE_NAMES[name]:{'Keys':unique[start:start+100],'ConsistentRead':True}}
        for attempt in range(6):
            result=_ddb_resource.batch_get_item(RequestItems=pending)
            output.extend(result.get('Responses',{}).get(TABLE_NAMES[name],[]))
            pending=result.get('UnprocessedKeys',{})
            if not pending:break
            time.sleep(min(0.05*2**attempt,1))
        else:raise ApiError('DB_UNAVAILABLE','Some records could not be read; retry the request',True)
    return output

def get_signing_key():
    global _secret_cache,_secret_cached_at
    if _secret_cache is not None and time.time()-_secret_cached_at<300:return _secret_cache
    arn=os.environ.get('TOKEN_SECRET_ARN')
    if not arn:raise ApiError('CONFIGURATION_ERROR','Token signing secret is not configured')
    response=_aws().client('secretsmanager',region_name=REGION).get_secret_value(SecretId=arn)
    try:key=base64.b64decode(json.loads(response['SecretString'])['hmacKey'],validate=True)
    except (KeyError,ValueError,TypeError):raise ApiError('CONFIGURATION_ERROR','Token signing secret is invalid')
    if len(key)<32:raise ApiError('CONFIGURATION_ERROR','Token signing secret is too short')
    _secret_cache,_secret_cached_at=key,time.time()
    return key

def _b64(value):return base64.urlsafe_b64encode(value).decode().rstrip('=')
def _unb64(value):return base64.urlsafe_b64decode(value+'='*(-len(value)%4))

def _seal(claims,audience,seconds):
    now=int(time.time())
    body={**claims,'v':1,'aud':audience,'iat':now,'exp':now+seconds}
    payload=_b64(canonical_json(body).encode('utf-8'))
    return payload+'.'+_b64(hmac.new(get_signing_key(),payload.encode('ascii'),hashlib.sha256).digest())

def _open(token,audience,error_code):
    if not isinstance(token,str) or len(token)>16384 or not re.fullmatch(r'[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}',token):raise ApiError(error_code,'Invalid or expired token')
    try:
        payload,signature=token.split('.')
        expected=hmac.new(get_signing_key(),payload.encode('ascii'),hashlib.sha256).digest()
        if not hmac.compare_digest(_b64(expected),signature):raise ValueError()
        claims=json.loads(_unb64(payload))
        now=int(time.time())
        if not isinstance(claims,dict) or claims.get('v')!=1 or claims.get('aud')!=audience:raise ValueError()
        if type(claims.get('exp')) is not int or type(claims.get('iat')) is not int:raise ValueError()
        if claims['exp']<=now or claims['iat']>now+30 or claims['exp']-claims['iat']>900:raise ValueError()
        return claims
    except ApiError:raise
    except (ValueError,TypeError,UnicodeError,KeyError):raise ApiError(error_code,'Invalid or expired token')

def _patient_claims(claims):
    require_dict(claims,'patient claims')
    validate_identifier(claims.get('patientKey'),'patientKey')
    validate_identifier(claims.get('datasetId'),'datasetId')
    systems=claims.get('sourceSystems')
    if not isinstance(systems,list) or not 1<=len(systems)<=16:raise ApiError('INVALID_PATIENT_TOKEN','Invalid source scope')
    for system in systems:validate_identifier(system,'sourceSystem')
    key=claims.get('patientResourceKey')
    if not isinstance(key,str) or not re.fullmatch(r'[^|]+\|Patient/[A-Za-z0-9.-]{1,64}',key) or key.split('|')[0] not in systems:
        raise ApiError('INVALID_PATIENT_TOKEN','Invalid patient resource scope')
    if type(claims.get('isSynthetic')) is not bool:raise ApiError('INVALID_PATIENT_TOKEN','Missing source classification')
    if not isinstance(claims.get('principalHash'),str) or not re.fullmatch('[a-f0-9]{64}',claims['principalHash']):
        raise ApiError('INVALID_PATIENT_TOKEN','Invalid caller binding')
    return claims

def binding_hash(value):return canonicalhash(value)

def seal_cursor(state,binding):
    require_dict(state,'cursor state')
    return _seal({'state':state,'binding':binding},CURSOR_AUD,CURSOR_SECONDS)

def open_cursor(token,binding):
    claims=_open(token,CURSOR_AUD,'INVALID_CURSOR')
    if claims.get('binding')!=binding or not isinstance(claims.get('state'),dict):raise ApiError('INVALID_CURSOR','Cursor does not match this caller or query')
    return claims['state']

def date_boundary(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
        raise ApiError('INVALID_REQUEST','Date filters must use YYYY-MM-DD (Asia/Seoul)')
    try:
        d=date.fromisoformat(value)
        return datetime(d.year,d.month,d.day,tzinfo=timezone(timedelta(hours=9))).astimezone(timezone.utc)
    except ValueError:raise ApiError('INVALID_REQUEST','Invalid calendar date')

def time_token(value):
    fraction=''
    try:
        if isinstance(value,datetime):
            parsed=value
            fraction=f'{parsed.microsecond:06d}'.rstrip('0') if parsed.microsecond else ''
        elif isinstance(value,str):
            if len(value)>256:raise ValueError()
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):parsed=date_boundary(value)
            else:
                match=re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})',value)
                if not match:raise ValueError()
                fraction=(match.group(1) or '').rstrip('0')
                parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        else:raise ValueError()
        if parsed.tzinfo is None or abs(parsed.utcoffset().total_seconds())>14*3600:raise ValueError()
        utc=parsed.astimezone(timezone.utc)
        return f'{utc.year:04d}{utc.month:02d}{utc.day:02d}{utc.hour:02d}{utc.minute:02d}{utc.second:02d}'+fraction+'#'
    except (ValueError,OverflowError):raise ApiError('INVALID_DATA','Invalid or unsupported exact FHIR date/time')

def reverse_token(token):return ''.join(f'{255-byte:02X}' for byte in token.encode('utf-8'))

def run_handler(event,context,dispatch):
    request_id=getattr(context,'aws_request_id',None) or str(uuid.uuid4())
    operation='unknown'
    started=time.monotonic()
    audit={'schemaVersion':2,'eventType':'carecompass.db.access','timestamp':now_iso(),
           'service':os.environ.get('AWS_LAMBDA_FUNCTION_NAME','carecompass-db'),
           'functionVersion':os.environ.get('AWS_LAMBDA_FUNCTION_VERSION','local'),
           'awsRequestId':request_id,'requestId':request_id,'operation':operation,
           'identitySource':'UNVERIFIED','principalHash':None,'patientKey':None,'datasetId':None,
           'outcome':'ERROR','errorCode':None,'authProvider':'COGNITO',
           'authOutcome':'NOT_CHECKED','authErrorCode':None,'revocationChecked':False}
    context_token=_audit_context.set(audit)
    auth_token=_auth_context.set(None)
    try:
        return _run_handler(event,context,dispatch,audit)
    finally:
        audit['durationMs']=round((time.monotonic()-started)*1000,3)
        try: _emit_audit(audit)
        finally:
            _audit_context.reset(context_token)
            _auth_context.reset(auth_token)

def _run_handler(event,context,dispatch,audit):
    request_id=audit['requestId']
    operation='unknown'
    try:
        require_dict(event,'request')
        if event.get('apiVersion')!=API_VERSION:raise ApiError('UNSUPPORTED_API_VERSION','apiVersion must be 2; Cognito access token required')
        if set(event)-{'apiVersion','operation','requestId','auth','params'}:raise ApiError('INVALID_REQUEST','Unknown request fields')
        requested_operation=event.get('operation')
        if not isinstance(requested_operation,str) or not re.fullmatch('[a-z][a-z0-9_]{0,63}',requested_operation):raise ApiError('INVALID_REQUEST','operation is invalid')
        operation=requested_operation
        audit['operation']=operation
        if event.get('requestId') is not None:
            value=event['requestId']
            try:
                if not isinstance(value,str):raise ValueError()
                request_id=str(uuid.UUID(value))
                audit['requestId']=request_id
            except (ValueError,TypeError,AttributeError):raise ApiError('INVALID_REQUEST','requestId must be a UUID')
        require_dict(event.get('params',{}),'params')
        if len(canonical_json(event).encode('utf-8'))>2_000_000:raise ApiError('REQUEST_TOO_LARGE','Request exceeds 2 MB')
        authenticate_request(event)
        _audit_targets(event.get('params',{}))
        data=native_json(dispatch(event))
        if len(canonical_json(data).encode('utf-8'))>3_000_000:raise ApiError('RESPONSE_TOO_LARGE','Use a smaller page or batch')
        _audit_result(data)
        audit['outcome']='SUCCESS'
        return {'apiVersion':API_VERSION,'requestId':request_id,'ok':True,'data':data}
    except ApiError as exc:error=exc
    except Exception as exc:
        response=getattr(exc,'response',{})
        code=response.get('Error',{}).get('Code','') if isinstance(response,dict) else ''
        retryable=code in {'ProvisionedThroughputExceededException','RequestLimitExceeded','ThrottlingException','InternalServerError','ServiceUnavailable','TransactionInProgressException'}
        if code:error=ApiError('DB_UNAVAILABLE' if retryable else 'DB_ACCESS_ERROR','Storage request failed',retryable)
        else:error=ApiError('INTERNAL_ERROR','Internal request processing failed')
        logger.error('handler_failure type=%s operation=%s requestId=%s',type(exc).__name__,operation,request_id)
    logger.info('request_error operation=%s requestId=%s code=%s',operation,request_id,error.code)
    audit['errorCode']=error.code
    audit['retryable']=error.retryable
    if error.code in {'UNAUTHORIZED','AUTH_REQUIRED','INVALID_ACCESS_TOKEN','ACCESS_TOKEN_INACTIVE',
                      'PATIENT_NOT_LINKED','PATIENT_LINK_INACTIVE','FORBIDDEN','RECORD_NOT_FOUND'}:
        audit['outcome']='DENIED'
    result={'code':error.code,'message':error.message,'retryable':error.retryable}
    if error.details is not None:result['details']=native_json(error.details)
    return {'apiVersion':API_VERSION,'requestId':request_id,'ok':False,'error':result}
