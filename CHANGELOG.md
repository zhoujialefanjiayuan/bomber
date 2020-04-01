# Change Log

更新日志（Change Log）是一个由人工编辑，以时间为倒叙的列表。这个列表记录所有版本的重大变动。  
如何维护更新日志 <https://keepachangelog.com/zh-CN/0.3.0/>   
参考范例 <https://gitlab.com/gitlab-org/gitlab-ce/blob/master/CHANGELOG.md>

## 2.10.1 (2018-08-25)
Modified (1 changed)
- 限制委外团队不能使用短信服务功能。http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/168

## 2.9.5 (2018-08-18)
Modified (1 chagned)
- Add save auto call list into history. http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/162

## 2.8.1
Fixed (1 changed)
- 调整外包代码逻辑。
- 修复呼出cycle1问题。

## 2.8.0
Added (1 changed)
- 新增一个催收日报 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/142
- 增加autocall history 记录表 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/143

## 2.7.0 (2018-07-23)
Added (1 changed)
- 外包件不进呼出队列

## 2.6.1 (2018-07-13)
Changed (1 changed)
- 将部分标记为本人的号码修改为fc，修改号码等级备注及source，亲戚号码被标记为本人。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/139

## 2.6.0 (2018-07-13)
Added (1 changed)
- 还款后 unpaid 值修改。对打款后的逻辑进行修改，在件被标记为已打款的同时修改 unpaid 值为 0 。用户还款后 unpaid 不为 0 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/137

## 2.5.0 (2018-07-14)
Added (1 changed)
- 测试短信到达率 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/135

## 2.4.1 (2018-07-07)
Changed (1 changed)
- 删除临时code

## 2.4.0 (2018-07-06)
Added (1 changed)
- 1、在golden_eye中找到同个ektp申请的不同账号的号码 2、从battlefront系统的user_login_log中个人的登陆记录找到该人所用过的设备，进而找到对方的号码 3、先找到device_no，再根据device_no从contact表中找到is_ec为true的号码 4、先找到device_no，再根据device_no分别从contact和call中寻找，分别找到该人的联系人和通话记录 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/132

## 2.3.0 (2018-06-29)
Added (1 changed)
- 1、判断出哪些件属于cycle1，查找该件在过去五天是否被打通过，找到ec联系人号码，加入呼出队列 2、从golden_eye得到数据，查找该号码是否已被加入contact表中，未加入则加入，已加入则修改号码所属的联系人群组 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/131

## 2.2.6 (2018-06-21)
Changed (2 changed)
- 少字段名冗余，较少数据包体积。数据中包含已同步过的数据时，全部数据无法同步成功。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/128

## 2.2.5 (2018-06-20)
Changed (1 change)
- 将利用消息通信改为利用http进行通信 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/125/diffs

## 2.2.4 (2018-06-19)
Fixed (1 chagne)
- 修改auto_call_list生成异常问题 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/124

## 2.2.3 (2018-06-14)
Changed (1 change)
- 将状态跟着另外一个定时任务走。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/121

## 2.2.2 (2018-06-14)
Added (1 change)
- 新增取得 SMS 状态消息发送。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/120

## 2.2.1 (2018-06-13)
Changed (1 change)
- 修改进入预测式呼出系统，根据逾期天数做排序。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/119

## 2.2.0 (2018-06-12)
Added (2 changes)
- 催收系统预测式呼出逻辑 —— 双卡加入‘本人队列’。从golden_eye项目中的device_user表中得到对应件的device_no，然后利用device_no从device_info表中得到对应的storage，从storage中筛选出号码传回bomber项目。利用auto_call_list对号码属性进行相应设置，使其能加入“本人”呼出队列。
- 将从golden_eye得到的号码，添加到contact表中，利用auto_call_list生成规则对号码属性进行相应设置，使其能加入“本人”呼出队列。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/116

Changed (1 change)
- 修改 c1 进入预测式呼出系统，根据逾期天数递减做排序。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/117/diffs

Fixed (1 change)
- bomber_auto_call_list 修改 application 排序规则 min to max。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/118

## 2.1.0 (2018-06-08)
Added (2 changes)
- 催收系统预测式呼出逻辑。实现将从battlefront获取的号码加入到contact。通过利用auto_call_list生成策略，将新号码的relationship设置为0，使其能加入“本人”呼出队列。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/115
- 将 worker 执行每一个都做记录，记下执行 message_id, result, time_spent。因为看到许多日志在 worker 有的跑很久、有的跑很多次，在日志里面不好做统计，也不好做修复。所以加入 worker_log 这功能，将每一个 worker 执行都记录下来，可以做出 worker 执行健康分析。 http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/114

Changed (1 change)
- update contact http://gitlab.pendanaan.com/hadoop-tech/bomber/merge_requests/113

## 2.0.0 (before 2018-06-04)