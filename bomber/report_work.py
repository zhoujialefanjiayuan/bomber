#-*- coding:utf-8 -*-

import logging
from decimal import Decimal
from datetime import timedelta,datetime,date

from bomber.db import readonly_db
from bomber.models import RepaymentReport

def run_all_sql(sql):
    try:
        cursor = readonly_db.get_cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
    except Exception as e:
        logging.info('run sql error:%s,sql:%s' % (str(e),sql))
        result = []
    return result

def calc_amount(sql_data,index=0):
    amount = 0
    if not sql_data:
        return amount
    try:
        for result in sql_data:
            amount += result[index] if result[index] else 0
    except Exception as e:
        logging.error("calc_amount,error:%s,data:%s"%(str(e),list(sql_data)))
    return amount

def repayment_report_create(report):
    all_money = Decimal(report["all_money"])
    repaid_money = Decimal(report["all_repaid"])
    type = report.get("type",0)
    contain_out = report.get("contain_out", 0)
    cycle = report.get("cycle",0)
    time = report.get("time")
    pro = 0
    if int(all_money):
        pro = round(repaid_money/all_money*100,2)
    RepaymentReport.create(
        cycle=cycle,
        type=type,
        time=time,
        all_money=all_money,
        repayment=repaid_money,
        proportion=pro,
        contain_out=contain_out
    )

# 得到分期dpd1-3的待催维度
def get_dpd_instalment_report(date_time):
    begin_time = str(date_time - timedelta(days=7))
    end_time = str(date_time)
    # 存量金额
    old_sql = """
    select sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
            as pending_amount
    from bomber.bomber_overdue bb
    inner join bill_java.overdue bo on bo.application_id = bb.external_id 
			 and bo.stage_num = bb.periods 
			 and bb.which_day = bo.which_day_overdue 
			 and bo.status=1
			 and bo.overdue_days in (2,3) 
			 and bo.stage_num is not null
    where bb.which_day = '%s' 
    and bb.cycle = 1
    """%begin_time
    old_data = run_all_sql(old_sql)
    old_money = calc_amount(old_data)
    # 新件金额
    new_sql = """
    select date(ba.dpd1_entry) as cdt, 
		   sum(o.principal_pending+o.late_fee_pending+o.interest_pending) 
			 as pending_amount
    from bomber.application ba
    inner join bomber.bomber_overdue bbo on ba.id=bbo.collection_id 
             and date(ba.dpd1_entry)=bbo.which_day
    inner join bill_java.overdue o on bbo.external_id = o.application_id
			 and o.stage_num = bbo.periods 
			 and bbo.which_day = o.which_day_overdue 
			 and o.status=1
    where ba.dpd1_entry >= '%s'
    and ba.dpd1_entry < '%s'
    and ba.type=1
    group by 1
    """%(begin_time, end_time)
    new_data = run_all_sql(new_sql)
    new_money = calc_amount(new_data,1)
    # 回款金额
    repayment_sql = """
    select date(repay_at),
		   sum(paid_amount)
    from (
        select br.application_id,br.repay_at,
               br.principal_part+br.late_fee_part as paid_amount,
               ba.id,ba.dpd1_entry
        from bomber.repayment_log br
        inner join bomber.application ba on br.application_id = ba.id 
          and (date(br.repay_at) <  date(ba.c1a_entry) or ba.c1a_entry is null)
        where br.repay_at>'%s' and br.repay_at<'%s' 
          and br.cycle=1
          and br.periods is not null
        group by 1,2 ) a
    group by 1
    """%(begin_time,end_time)
    repaid_data = run_all_sql(repayment_sql)
    repaid = calc_amount(repaid_data,1)
    all_money = round((old_money+new_money)/1000000,2)
    all_repaid = round( repaid/1000000,2)
    report = {"type": 1,
              "time": begin_time,
              "all_money": all_money,
              "all_repaid": all_repaid}
    repayment_report_create(report)

# 获取c1a的分期的待催维度报表
def get_c1a_instalment_report(date_time):
    begin_time = str(date_time - timedelta(days=7))
    end_time = str(date_time)
    # 存量金额
    old_sql = """
    select sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
        as pending_amount
    from bomber.bomber_overdue bb
    inner join bill_java.overdue bo on bo.application_id = bb.external_id 
                 and bo.stage_num = bb.periods 
                 and bb.which_day = bo.which_day_overdue 
                 and bo.status=1
                 and bo.overdue_days > 4
                 and bo.stage_num is not null
    where bb.which_day = '%s' 
    and bb.cycle = 1
    """%begin_time
    old_data = run_all_sql(old_sql)
    old_money = calc_amount(old_data)
    # 新件金额
    new_sql = """
    select date(ba.c1a_entry) as cdt, 
		   sum(o.principal_pending+o.late_fee_pending+o.interest_pending) 
		    as pending_amount
    from bomber.application ba
    inner join bomber.bomber_overdue bbo on ba.id=bbo.collection_id 
        and date(ba.c1a_entry)=bbo.which_day
    inner join bill_java.overdue o on bbo.external_id = o.application_id
                 and o.stage_num = bbo.periods 
                 and bbo.which_day = o.which_day_overdue 
                 and o.status=1
    where ba.c1a_entry >= '%s'
    and ba.c1a_entry < '%s'
    and ba.type=1
    group by 1
    """%(begin_time, end_time)
    new_data = run_all_sql(new_sql)
    new_money = calc_amount(new_data,1)
    # 回款金额
    repaid_sql = """
    select date(repay_at),sum(paid_amount)
    from (
        select br.application_id,
          br.repay_at,br.principal_part+br.late_fee_part as paid_amount,ba.id
        from bomber.repayment_log br
        inner join bomber.application ba on br.application_id = ba.id 
            and date(br.repay_at)>= date(ba.c1a_entry)
        where br.repay_at>'%s' and br.repay_at<'%s' 
        and br.cycle=1
        and br.periods is not null
        group by 1,2 ) a
    group by 1
    """%(begin_time, end_time)
    repaid_data = run_all_sql(repaid_sql)
    repaid = calc_amount(repaid_data,1)
    all_money = round((old_money + new_money) / 1000000, 2)
    all_repaid = round(repaid / 1000000, 2)
    report = {"cycle": 1,
              "time": begin_time,
              "all_money": all_money,
              "all_repaid": all_repaid,
              "type": 1}
    repayment_report_create(report)

# 获取分期每个cycle的回款金额
def get_repaid_instalment_report(date_time,contain_out=True):
    begin_time = str(date_time - timedelta(days=7))
    end_time = str(date_time)
    cycle_list = [2,3,4]
    result = dict.fromkeys(cycle_list,0)
    for cycle in cycle_list:
        if contain_out:
            repaid_sql = """
            select date(repay_at),sum(paid_amount)
            from (
                select br.application_id,br.repay_at,
                     br.principal_part+br.late_fee_part as paid_amount
                from bomber.repayment_log br
                where br.repay_at>'%s' and br.repay_at<'%s'
                    and br.cycle=%s
                    and br.periods is not null
                    group by 1,2 ) a
            group by 1
            """%(begin_time,end_time,cycle)
        else:
            repaid_sql = """
            select date(repay_at),sum(paid_amount)
            from (
                select br.application_id,br.repay_at,
                     br.principal_part+br.late_fee_part as paid_amount
                from bomber.repayment_log br
                where br.repay_at>'%s' and br.repay_at<'%s'
                    and br.cycle=%s
                    and br.periods is not null
                    and not exists(
                        select 1 
                        from bomber.bomber bb 
                        where br.current_bomber_id = bb.id 
                            and bb.role_id = 11)
                    group by 1,2 ) a
            group by 1
            """ % (begin_time, end_time, cycle)
        repaid_data = run_all_sql(repaid_sql)
        repaid = calc_amount(repaid_data,1)
        result[cycle] = round(repaid/1000000,2)
    return result

# 获取分期1b,2,3的总金额
def get_all_instalment_report(date_time,contain_out=True):
    begin_time = str(date_time - timedelta(days=7))
    end_time = str(date_time)
    cycle_list = [2,3,4]
    where_list = ['ba.c1b_entry', 'ba.c2_entry', 'ba.c3_entry']
    result = dict.fromkeys(cycle_list, 0)
    for index,cycle in enumerate(cycle_list):
        condition = where_list[index]
        if contain_out:
            old_sql = """
            select 
              sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending)
                as pending_amount
            from bomber.bomber_overdue bb
            inner join bill_java.overdue bo on bo.application_id = bb.external_id 
                     and bo.stage_num = bb.periods 
                     and bb.which_day = bo.which_day_overdue 
                     and bo.status=1
                     and bo.stage_num is not null
            where bb.which_day = '%s' 
            and bb.cycle = %s
            and not exists(
                select 1 
                from bomber.application ba 
                where bb.collection_id = ba.id 
                  and bb.which_day = date(%s))
            """%(begin_time, cycle, condition)
            new_sql = """
            select date(%s) as cdt, 
			    sum(o.principal_pending+o.late_fee_pending+o.interest_pending) 
			        as pending_amount
            from bomber.application ba
            inner join bomber.bomber_overdue bbo on ba.id=bbo.collection_id 
                    and date(%s)=bbo.which_day
            inner join bill_java.overdue o on bbo.external_id = o.application_id
                    and o.stage_num = bbo.periods 
                    and bbo.which_day = o.which_day_overdue 
                    and o.status=1
                    and o.stage_num is not null
            where %s >= '%s'
                and %s < '%s'
                and ba.type=1
            group by 1
            """%(condition,condition,condition,begin_time,condition,end_time)
        else:
            old_sql = """
            select 
              sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
                as pending_amount
            from bomber.bomber_overdue bb
            inner join bill_java.overdue bo on bo.application_id = bb.external_id 
                         and bo.stage_num = bb.periods 
                         and bb.which_day = bo.which_day_overdue 
                         and bo.status=1
                         and bo.stage_num is not null
            where bb.which_day = '%s' 
            and bb.cycle = %s
            and not exists(select 1 
                           from bomber.application ba 
                           where ba.id = bb.collection_id
                                and date(%s) = bb.which_day)
            and not exists(select 1
                           from bomber.dispatch_app_history bdh 
                           inner join  bomber.partner bp on bdh.partner_id = bp.id 
                                and bp.cycle= %s
                           where bb.collection_id=bdh.application_id 
                                and entry_at >= '%s' 
                                and entry_at< date_add('%s', interval 1 month))         
            """%(begin_time, cycle,condition,cycle,begin_time,begin_time)
            new_sql = """
            select date(%s) as cdt, 
			    sum(o.principal_pending+o.late_fee_pending+o.interest_pending) 
			        as pending_amount
            from bomber.application ba
            inner join bill_java.overdue o on ba.id=o.application_id 
                and date(%s)=date(o.which_day_overdue) 
                and o.status=1
            where %s >= '%s'
                and %s < '%s'
                and not exists(
                      select 1 
                      from bomber.dispatch_app_history bdh 
                      inner join  bomber.partner bp on bdh.partner_id = bp.id 
                            and bp.cycle=%s
                      where ba.id=bdh.application_id 
                            and date(%s) = date(bdh.entry_at) )     
                and ba.type=1
            group by 1
            """ % (condition, condition, condition, begin_time, condition,
                   end_time, cycle, condition,)
            old_data = run_all_sql(old_sql)
            old_money = calc_amount(old_data)
            new_data = run_all_sql(new_sql)
            new_money = calc_amount(new_data,1)
            all_money = round((old_money+new_money)/1000000,2)
            result[cycle] = all_money
        return result

# 现金贷
def get_dpd_report(date_time):
    begin_time = str(date_time - timedelta(7))
    end_time = str(date_time)
    # 存量金额
    old_sql = """
    select sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
      as pending_amount
    from bill_java.overdue bo 
    where bo.which_day_overdue = '%s'
        and bo.status=1
        and bo.overdue_days in (2,3)
        and bo.stage_num is null
    """%(begin_time)
    old_datda = run_all_sql(old_sql)
    old_money = calc_amount(old_datda)
    # 新件金额
    new_sql = """
    select date(o.which_day_overdue) as cdt, 
		   sum(o.principal_pending+o.late_fee_pending+o.interest_pending) 
		    as pending_amount
    from bill_java.overdue o 
    where o.which_day_overdue >= '%s'
        and o.which_day_overdue < '%s'
        and o.status=1 
        and o.overdue_days=1
        and o.stage_num is null
    group by 1
    """%(begin_time, end_time)
    new_data = run_all_sql(new_sql)
    new_money = calc_amount(new_data,1)
    # 回款
    repaid_sql = """
    select date(which_day_overdue),sum(amount_paid) 
    from (
        select bo1.application_id,
              (br.principal_part+late_fee_part) as amount_paid,br.repay_at,
              which_day_overdue
        from bill_java.overdue bo1 
        inner join bomber.repayment_log br on bo1.application_id = br.application_id 
            and date(date_sub(br.repay_at,interval 2 hour)) = bo1.which_day_overdue
        where bo1.overdue_days in (1,2,3) and bo1.status=1
            and bo1.which_day_overdue>='%s'
            and bo1.which_day_overdue<'%s'
            and bo1.stage_num is null
        group by 1,3) a 
    group by 1
    """%(begin_time, end_time)
    repaid_data = run_all_sql(repaid_sql)
    repaid = calc_amount(repaid_data,1)
    all_money = round((old_money+new_money)/1000000,2)
    all_repaid = round(repaid/1000000,2)
    report = {"time": begin_time,
              "all_money": all_money,
              "all_repaid": all_repaid}
    repayment_report_create(report)

# 统计c1a消息
def get_c1a_report(date_time):
    begin_time = str(date_time - timedelta(7))
    end_time = str(date_time)
    # 存量金额
    old_sql = """
    select sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
            as pending_amount
    from bomber.application ba
    inner join bill_java.overdue bo on ba.id=bo.application_id 
          and bo.which_day_overdue='%s' 
          and bo.status=1 
          and bo.overdue_days>3
    where ba.c1a_entry< '%s'
          and ba.c1a_entry> date_sub('%s',interval 14 day)
          and (ba.c1b_entry is null or ba.c1b_entry>date_add('%s',interval 1 day))
          and ba.type=0
    """%(begin_time, begin_time, begin_time, begin_time)
    old_data = run_all_sql(old_sql)
    old_money = calc_amount(old_data)
    # 新件
    new_sql = """
    select date(ba.c1a_entry) as cdt,
		   sum(o.principal_pending+o.late_fee_pending+o.interest_pending) 
		      as pending_amount
    from bomber.application ba
    inner join bill_java.overdue o on ba.id=o.application_id 
          and date(ba.c1a_entry)=date(o.which_day_overdue) 
          and o.status=1 
          and o.overdue_days=4 
    where ba.c1a_entry >= '%s'
          and ba.c1a_entry < '%s'
          and ba.type=0
    group by 1
    """%(begin_time, end_time)
    new_data = run_all_sql(new_sql)
    new_money = calc_amount(new_data,1)
    # 回款
    repaid_sql = """
    select date(repay_at),
			 sum(paid_amount)
    from (
        select br.application_id,br.repay_at,
            br.principal_part+br.late_fee_part as paid_amount
        from bomber.repayment_log br
        inner join bomber.application ba on br.application_id = ba.id 
            and date(br.repay_at)>= date(ba.c1a_entry)
        where br.repay_at>'%s' and br.repay_at<'%s' 
            and br.cycle=1
            and br.periods is null 
        group by 1,2 ) a
    group by 1
    """%(begin_time,end_time)
    repaid_data = run_all_sql(repaid_sql)
    repaid_money = calc_amount(repaid_data,1)
    all_money = round((old_money+new_money)/1000000,2)
    all_repaid = round(repaid_money/1000000,2)
    report = {"cycle": 1,
              "time": begin_time,
              "all_money": all_money,
              "all_repaid": all_repaid}
    repayment_report_create(report)

# 获取每个cycle的回款
def get_repaid_report(date_time, contain_out=True):
    begin_time = str(date_time - timedelta(7))
    end_time = str(date_time)
    cycle_list = [2,3,4]
    result = dict.fromkeys(cycle_list, 0)
    for cycle in cycle_list:
        if contain_out:
            repaid_sql = """
            select date(repay_at),sum(paid_amount)
            from (
                select br.application_id,br.repay_at,
                        br.principal_part+br.late_fee_part as paid_amount
                from bomber.repayment_log br
                where br.repay_at>'%s' and br.repay_at<'%s'
                    and br.cycle=%s
                    and br.periods is null
                    and br.no_active=0
                group by 1,2 ) a
            group by 1
            """%(begin_time, end_time, cycle)
        else:
            repaid_sql = """
            select date(repay_at),sum(paid_amount)
            from (
                select br.application_id,br.repay_at,
                      br.principal_part+br.late_fee_part as paid_amount
                from bomber.repayment_log br
                where br.repay_at>'%s' and br.repay_at<'%s'
                and br.cycle=%s
                and br.no_active=0
                and not exists(
                                select 1 
                                from bomber.bomber bb 
                                where br.current_bomber_id = bb.id 
                                  and bb.role_id = 11)
                and br.periods is null
                group by 1,2 ) a
            group by 1
            """%(begin_time, end_time, cycle)
        repaid_data = run_all_sql(repaid_sql)
        repaid_money = calc_amount(repaid_data, 1)
        result[cycle] = round(repaid_money/1000000,2)
    return result

# 现金贷每个cycle的统计
def get_all_money_report(date_time,contain_out=True):
    begin_time = str(date_time - timedelta(7))
    end_time = str(date_time)
    month_time = end_time[:-2]+'01'
    cycle_list = [2,3,4]
    entry_list = ['ba.c1b_entry', 'ba.c2_entry','ba.c3_entry']
    days = [30, 60, 60]
    result = dict.fromkeys(cycle_list, 0)
    for index,cycle in enumerate(cycle_list):
        entry_1 = entry_list[index]
        if cycle != 4:
            entry_2 = entry_list[index+1]
            condtion = """%s is null or %s>date_add('%s',interval 1 day)"""%(
                entry_2,entry_2,begin_time)
        else:
            condtion = """bo.overdue_days<=90"""
        day = days[index]
        if contain_out:
            old_sql = """
            select 
              sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
                as pending_amount
            from bomber.application ba
            inner join bill_java.overdue bo on ba.id=bo.application_id 
              and bo.which_day_overdue='%s' 
              and bo.status=1
            where %s< '%s'
              and %s> date_sub('%s',interval %s day)
              and (%s)
              and ba.type=0
            """%(begin_time,entry_1,begin_time,entry_1,begin_time,day,condtion)
            new_sql = """
            select date(ba.c1b_entry) as cdt, 
			      sum(o.principal_pending+o.late_fee_pending+o.interest_pending)
			       as pending_amount
            from bomber.application ba
            inner join bill_java.overdue o on ba.id=o.application_id 
                and date(%s)=date(o.which_day_overdue)
                and o.status=1
            where %s >= '%s'
                and %s < '%s'
                and ba.type=0
            group by 1
            """%(entry_1,entry_1,begin_time,entry_1,end_time)
        else:
            old_sql = """
            select 
              sum(bo.principal_pending+bo.late_fee_pending+bo.interest_pending) 
                as pending_amount
            from bomber.application ba
            inner join bill_java.overdue bo on ba.id=bo.application_id 
              and bo.which_day_overdue='%s' 
              and bo.status=1
            where %s< '%s'
              and %s> date_sub('%s',interval %s day)
              and (%s)     
              and not exists(
                  select 1 
                  from bomber.dispatch_app_history bdh 
                  inner join  bomber.partner bp on bdh.partner_id = bp.id 
                    and bp.cycle= %s
                  where ba.id=bdh.application_id 
                    and entry_at >= '%s' 
                    and entry_at < date_add('%s', interval 1 month))
              and ba.type=0 
            """%(begin_time,entry_1,begin_time,entry_1,begin_time,day,condtion,
                 cycle,month_time,month_time)
            new_sql = """
            select date(%s) as cdt, 
			   sum(o.principal_pending+o.late_fee_pending+o.interest_pending)
			      as pending_amount
            from bomber.application ba
            inner join bill_java.overdue o on ba.id=o.application_id 
                and date(%s)=date(o.which_day_overdue) 
                and o.status=1
            where %s >= '%s'
                and %s < '%s'
                and not exists(
                    select 1 
                    from bomber.dispatch_app_history bdh 
                    inner join  bomber.partner bp on bdh.partner_id = bp.id 
                       and bp.cycle=%s
                    where ba.id=bdh.application_id 
                       and date(%s) = date(bdh.entry_at))
                and ba.type=0
            group by 1
            """%(entry_1, entry_1, entry_1, begin_time, entry_1, end_time,
                 cycle, entry_1)
        old_data = run_all_sql(old_sql)
        old_money = calc_amount(old_data)
        new_data = run_all_sql(new_sql)
        new_money = calc_amount(new_data,1)
        all_money = round((old_money+new_money)/1000000,2)
        result[cycle] = all_money
    return result

# 获取每个cycle的报表
def get_every_cycle_report(date_time):
    time = str(date_time - timedelta(days=7))
    # dpd,1a现金贷报表
    get_dpd_report(date_time)
    get_c1a_report(date_time)
    # dpd,1a分期报表
    get_dpd_instalment_report(date_time)
    get_c1a_instalment_report(date_time)
    # 分期报表
    ins_inner_repaid = get_repaid_instalment_report(date_time, False)
    ins_all_repaid = get_repaid_instalment_report(date_time)
    ins_inner_all_money = get_all_instalment_report(date_time, False)
    ins_all_all_money = get_all_instalment_report(date_time)
    # 现金贷报表
    inner_repaid = get_repaid_report(date_time, False)
    all_repaid = get_repaid_report(date_time)
    inner_all_money = get_all_money_report(date_time, False)
    all_money = get_all_money_report(date_time)
    cycle_list = [2,3,4]
    for cycle in cycle_list:
        ins_inner_report = {"type": 1,
                            "time": time,
                            "cycle": cycle,
                            "all_repaid": ins_inner_repaid[cycle],
                            "all_money": ins_inner_all_money[cycle]}
        repayment_report_create(ins_inner_report)
        ins_all_report = {"type": 1,
                          "time": time,
                          "cycle": cycle,
                          "contain_out": 1,
                          "all_repaid": ins_all_repaid[cycle],
                          "all_money": ins_all_all_money[cycle]}
        repayment_report_create(ins_all_report)
        inner_report = {"time": time,
                        "cycle": cycle,
                        "all_repaid": inner_repaid[cycle],
                        "all_money": inner_all_money[cycle]}
        repayment_report_create(inner_report)
        all_report = {"time":time,
                      "cycle": cycle,
                      "contain_out": 1,
                      "all_repaid": all_repaid[cycle],
                      "all_money": all_money[cycle]}
        repayment_report_create(all_report)
