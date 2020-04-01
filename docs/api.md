**host**
----
* **host**
``http://test-bomber.pendanaan.com``

**login**
----
* **URL**

  `POST` `/api/v1/login`

* **Json Params**

  ```json
  {
    "username": "admin",
    "password": "123123"
  }
  ```
  
* **Note**
  登录之后，所有的请求`headers`添加
  ```text
  Authorization: Bearer 'jwt'
  ```
  
**reset password**
----
* **URL**

  `PATCH` `/api/v1/reset-password`

* **Json Params**

  ```json
  {
    "old_password": "admin",
    "new_password": "123123"
  }
  ```
  
**permission**
----
* **URL**

  `GET` `/api/v1/permission`
  
  
**logout**
----
* **URL**

  `GET` `/api/v1/logout`
  
  
**unclaimed**
----
* **URL**

  `GET` `/api/v1/applications/unclaimed`
  
**claim**
----
* **URL**

  `POST` `/api/v1/applications/claim`

* **Json Params**

  ```json
  
  {
    "claimed_apps": [1449746311095128065]
  }

  ```
  
**bombers**
----
获取所有员工,`processing`搜索会用到
* **URL**

  `GET` `/api/v1/bombers`
  
**processing**
----
* **URL**

  `GET` `/api/v1/applications/processing`

* **URL Params**

  `application_id` `user_id` `user_name` `mobile` 
  `promised(promised=0, unpromised=1)` `last_collector_id` 
  `page`
  
**repaid**
----
* **URL**

  `GET` `/api/v1/applications/repaid`

* **URL Params**

  `application_id` `user_id` `user_name` `mobile` 
  `page`
  
**application detail**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>`

**contacts**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/contacts/<relationship:int>`
  
* **Note** 
  ```text
  relationship:

  APPLICANT = 0
  FAMILY = 1
  COMPANY = 2
  SUGGESTED = 3
  ```

**add contacts**
----
* **URL**

  `POST` `/api/v1/applications/<app_id:int>/contacts`
  
* **Json Params**

  ```json
  
  {
    "name": "enomine",
    "number": "12314345",
    "relationship": 0,
    "remark": ""
  }

  ```
  
**connect_history**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/connect_history`
  
* **URL Params**

  `number` `type`
  
**get sms template**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/templates/sms`
  
**get call template**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/templates/call`

**add call action**
----
* **URL**

  `POST` `/api/v1/applications/<app_id:int>/contacts/<contact_id:int>/call`
  
* **Json Params**

  ```json
  
  {
    "remark": "remark",
    "status": 1,
    "relationship": 1,
    "sub_relation": 0
  }

  ```
  
**add sms action**
----
* **URL**

  `POST` `/api/v1/applications/<app_id:int>/contacts/<contact_id:int>/sms`
  
* **Json Params**

  ```json
  
  {
    "template_id": 1,
    "text": "hello"
  }

  ```
  
**phone-verify[underwriting]**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/questions/<cat>`
  
* **Notes**
  ```text
  cat [applicant, family, company, summary]
  ```
  
**get-loan-history**
----

* **URL**

  `GET` `/api/v1/applications/<app_id:int>/loan-history`

* **URL Params**

  `page`
  
* **Response**
  ```text
  "late_fee": "202000.00",
  "late_fee_initial": "20000.00",
  "id": "1494713150577184768",
  "user_id": "1454402595425751040",
  "overdue_days": 13,
  "due_at": "2017-07-20 22:00:00",
  "status": 0,
  "finished_at": null,
  "latest_repay_at": null,
  "origin_due_at": "2017-07-20 22:00:00",
  "late_fee_rate": "0.0140",
  "principal_paid": "0.00",
  "created_at": "2017-07-06 19:46:39",
  "external_id": "203860",
  "updated_at": "2017-08-02 02:00:02",
  "principal": "1000000.00",
  "min_pending_amount": null,
  "late_fee_paid": "0.00"
  ```
* **Notes**
  ```text
  LID: id, UID: user_id, disburse time: created_at, due time: due_at
  repaid time: latest_repay_at, overdue: overdue_days, apply: principal,
  late fee: late_fee, repaid: principal_paid+late_fee_paid, 
  unpaid: principal + late_fee - repaid
  repaid_at: latest_repay_at
  ```

**get collection history**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/collection_history`
  
* **URL Params**

  `page`
  
**get user collection history**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/user_collection_history`
  
* **URL Params**

  `page`
  
  
**add collection history**
----
* **URL**

  `POST` `/api/v1/applications/<app_id:int>/collection_history`
  
* **Json Params**

  ```json
  {
      "promised_amount": "100.52",
      "promised_date": "2017-08-08",
      "follow_up_date": "2017-08-08 12:00:00",
      "result": "123",
      "remark": "1234"
  }
  ```
  
**get escalation**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/escalations`
  
* **URL Params**

  `page`
  
  
**get transfers**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/transfers`
  
* **URL Params**

  `page`
  
  
**get pending transfers**
----
* **URL**

  `GET` `/api/v1/transfers/pending`
  
* **URL Params**

  `page`
  
  
**add transfers**
----
* **URL**

  `POST` `/api/v1/transfers`
  
* **Json Params**

  ```json
  {
      "application_list": [337636],
      "transfer_to": 1,
      "reason": "transfer"
  }
  ```
  
**review transfers**
----
* **URL**

  `PATCH` `/api/v1/transfers`
  
* **Json Params**

  ```json
  {
      "transfer_list": [1509892533197606913], 
      "status": 1
  }
  ```

**get reject history**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/rejects`
  
* **URL Params**

  `page`
  
  
**get pending reject**
----
* **URL**

  `GET` `/api/v1/rejects/pending`
  
* **URL Params**

  `page`
  
  
**add reject**
----
* **URL**

  `POST` `/api/v1/application/<app_id:int>/reject-lists`
  
* **Json Params**

  ```json
  {
      "reason": "reject"
  }
  ```
  
**review reject**
----
* **URL**

  `PATCH` `/api/v1/reject-lists`
  
* **Json Params**

  ```json
  {
      "reject_list": [1510270683001786369], 
      "status": 1
  }
  ```
  
**get discount history**
----
* **URL**

  `GET` `/api/v1/applications/<app_id:int>/discounts`
  
* **URL Params**

  `page`
  
  
**get pending discount**
----
* **URL**

  `GET` `/api/v1/discounts/pending`
  
* **URL Params**

  `page`
  
**add discount**
----
* **URL**

  `POST` `/api/v1/application/<app_id:int>/discount`
  
* **Json Params**

  ```json
  {
      "discount_to": "120.00",
      "effective_to": "2017-10-11",
      "reason": "reject"
  }
  ```
  
**review discount**
----
* **URL**

  `PATCH` `/api/v1/discount`
  
* **Json Params**

  ```json
  {
      "discount_list": [1510270683001786369], 
      "status": 1
  }
  ```
  
**repayment calculate tool**
----
  
* **Note**
  ```text
  用application的接口, 获取
  amount, late_fee_rate, late_fee_initial, late_fee, due_at, principal_paid, late_fee_paid
  弹出框选择 repay_date
  cal_due_at = due_at < now() ? now():due_at
  late fee = late_fee + max((amount - principal_paid) * late_fee_rate * (repay_date - cal_due_at), 0) - late_fee_paid
  need_repay = (amount-principal_paid) + late fee
  ```
  
**get dept statics**
----
* **URL**

  `GET` `/api/v1/department/summary`
  
* **URL Params**

  `start_date` `end_date` `bomber_id` `export`
 
**get statics chart**
----
* **URL**

  `GET` `/api/v1/personal/summary/chart`
  
* **URL Params**

  `start_date` `end_date` 
 
**get inbox**
----
* **URL**

  `GET` `/api/v1/inbox/unread`
  
**read inbox**
----
* **URL**

  `GET` `/api/v1/inbox/read/<category>`

**personal summary**
----
* **URL**

  `GET` `/api/v1/personal/summary`
  
