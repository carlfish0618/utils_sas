%LET my_dir = D:\Research\DATA\yjyg_link;
%LET output_dir = &my_dir.\output_data;
%LET input_dir = &my_dir.\input_data;


/* create library */
LIBNAME yl "&my_dir.\sasdata";

DATA subset(rename = (efctid = event_id end_date = date));
	SET yl.merge_stock_pool(keep = efctid stock_code end_date);
RUN;

PROC SORT DATA = subset NODUPKEY;
	BY event_id;
RUN;




/*����ʾ��:*/
/* 
����:
(1) ָ���߼���: ��ʱ�߼���(work)
(2) �¼�������Ϊ: event (����������)
(3) �¼��о����ɵ��ⲿ�ĵ��洢��: D:\Research\Data\output_data �ļ�����
(4) �¼��о�������Ҫ�������ⲿ�ĵ��洢��: D:\Research\Data\input_data �ļ�����
(5) �Ƚϻ�׼Ϊ: ����300 
*/

/* ��1��:
�������ݿ�: initial_sas.sas�ļ� 
����: environment_set.sas �ļ�
(�������ᵽ�����в��������ڸ��ļ������Ϸ����������á��У����Ӧ�޸ġ�һ��Ҫ��ָ֤�����ļ��д���)
*/

%INCLUDE  "D:\Research\CODE\sascode\event\��������\initial_sas.sas";
%INCLUDE  "D:\Research\CODE\sascode\event\��������\environment_set_2.sas";

/*
��2��:
׼�����¼��ļ�(��������Ϊ: subset, �����Ѿ�������ָ�����߼����У���work.subset)
����������ֶ���: event_id, date, stock_code
����: event_id Ҫ����Ψһ��

����: dateҪ���ǽ����ա�
������ǽ����գ���ʹ��date_macro.sas�е�adjust_date�������е�����
*/

/* ��Ӧ�Ĵ���Ϊ: */
%adjust_date(busday_table = &my_library..busday , raw_table = &my_library..subset ,colname = date); 
DATA &my_library..subset_neat(drop = adj_date);
	SET &my_library..subset(drop = date date_is_busday);;
	date = adj_date;
	FORMAT date mmddyy10.;
RUN;

/* ȥ�أ�һֻ��Ʊ��һ��ֻ��һ����¼ */
/* ��������Ӱ��֮��ÿ�������ʵļ��㣬����һ��Ҫע�⣡��*/
PROC SORT DATA = &my_library..subset_neat NODUPKEY;
	BY stock_code date;
RUN;


/* 
��3��:�¼��о�  
���裺
(1) ��ע: �¼�����[-10,60]�ĵ���������(start_win = 10, end_win = 60)
(2) �ۼ�������: ���¼���(buy_win = 0)�п�ʼ����
(3) alphaͳ���ļ��洢��alpha_result.xls�У�ע�⣺�����е�д�룬Ҫ����ļ��е�������д���Sheet�����Ѿ����ڣ���: daily_alpha/accum_alpha/accum_alpha_after�����Ѿ������ļ��У�
*/

%LET start_win = -10;
%LET end_win = 60;
%LET buy_win = 0;

%MACRO gen_module(bm_name, is_group, group_var, filename);

	/* �¼����� */
	%gen_overlapped_win(my_library = &my_library, eventName = &eventName., eventName_strategy = subset_neat, 
			my_hqinfo_with_bm = my_hqinfo_with_&bm_name., start_win = &start_win., end_win = &end_win.);
	/* ����alpha */
	%cal_access_ret(my_library = &my_library, eventName = &eventName., eventName_hq = &eventName._hq,  buy_win = &buy_win.,
		trading_table = trading_table, list_delist_table=list_delist_table);
	/* ���� */
	%attribute_to_event(my_library = &my_library, alpha_file = &eventName._alpha , eventName_strategy = subset_neat, 
		output_file = &eventName._alpha , stock_sector_mapping = stock_sector_mapping, stock_bk_mapping = stock_bk_mapping);
	/* ͳ�� */
	%alpha_collect(my_library = &my_library, alpha_file = &eventName._alpha, 
		alpha_var = accum_alpha, is_group = &is_group., group_var = &group_var. ,filename = &filename, sheetname = &group_var._&bm_name.);
%MEND;



/* ģ��1: ��ҵ */
%gen_module(bm_name=bm, is_group=1, group_var=o_name, filename=&eventName._o_name.xls)
%gen_module(bm_name=indus, is_group=1, group_var=o_name, filename=&eventName._o_name.xls)

/* ģ��2: ��� */
%gen_module(bm_name=bm, is_group=1, group_var=year, filename=&eventName._year.xls)
%gen_module(bm_name=indus, is_group=1, group_var=year, filename=&eventName._year.xls)

/* ģ��3: ��� */
%gen_module(bm_name=bm, is_group=1, group_var=bk, filename=&eventName._bk.xls)
%gen_module(bm_name=indus, is_group=1, group_var=bk, filename=&eventName._bk.xls)



/* ���ջ�����ָ��������ļ���·���У������ļ�: alpha_result.xls�����а�������Sheet, �ֱ�Ϊ: daily_alpha/accum_alpha/accum_alpha_after */
/* ��ϣ���Բ�ͬ���ڵ�alpha������ͳ�Ʒ�ʽ������ֱ�Ӷ�: &eventName._&start_win._&end_win. �ļ�����ͳ�Ʒ����� */

/* ��4��: �����Ȩ��� */
/* ���裺
	(1) ׼�����ļ�: cur_event(��������������Ҫ������ָ�����߼����У���: work.cur_event��
		Ҫ��: cur_event�ĽṹΪ
		event_id: �¼�id(Ψһ)
		date������
		stock_code
		max_day������н���������
		min_day: ��̳�������(��ʱû���õ�)
		ineffective_date: ʧЧ��(����ʱ�����������ָ��Ϊĳһ������)
		score: ��������Ȩ�أ���Ȩֻ��Ҫ��ÿ��������Ϊ1����
	��2) max_weight: �������Ȩ��
*/

%LET max_weight = 0.05;


/* ���û������Ҫ�� ������֮ǰ��work.subset_neat�ļ������Ƹ��ļ� */
DATA &my_library..cur_event;
	SET &my_library..subset_neat;
	max_day = 60;  /* ����60�������� */
	min_day = .;
	ineffective_date = '31dec2100'd;  /* ����Ϊδ����Զ����������ʾ�ò�����ʱʧЧ */
	FORMAT ineffective_date mmddyy10.;
	score = 1;
RUN;

/* ɾ�����¼��յ���۸�ȱʧ�ļ�¼ */
%filter_event(my_library = &my_library, event_table = cur_event, delist_table = stock_delist_table, output_table = cur_event);

/* ���ɹ�Ʊ�� */
%equally_weighted_stock_pool(my_library = &my_library, eventName = &eventName., event_table = cur_event,
	weight_function  = score, busday_table = &my_library..busday, delist_table = stock_delist_table);

/* ��׼��Ȩ�� */
%norm_weight(my_library = &my_library, stock_pool = &eventName._pool, 
		is_fixed_size = 0, size =0 , stock_pool_norm_weight=&eventName._pool_edit);

/* �������Ȩ�� */
%adjust_weight(my_library = &my_library, stock_pool = &eventName._pool_edit, 
	max_weight = &max_weight., is_other_adjust = 0, stock_pool_adjust_weight = &eventName._pool_edit)

DATA &my_library..&eventName._pool_edit;
	SET &my_library..&eventName._pool_edit(drop =  event_day event_id sell_date last_sell_date last_event_id);
RUN;

/* �������ɵ��ļ��洢��: &eventName._pool_eidt�� */
/* is_buy = 1, is_sell = 0: ������������ */
/* is_buy = 0, is_sell = 0: ���� */
/* is_buy = 0, is_sell = 1: ������������ */



/* ��5��:����ÿ�������� */
/* ����:
	(1) ������ʼ�ͽ������� 
	(2) ����λδ��������ָ��(benchmark_code)���������ͬʱalpha�Ļ�׼ҲѡΪ(benchmark_code) 
*/

%LET strategy_start = 25dec2006;
%LET strategy_end = 31mar2014;

DATA busday_need;
	SET &my_library..busday;
	IF "&strategy_start."d<=date<="&strategy_end."d;
RUN;

%cal_holdings_for_all_equally(my_library = &my_library, stock_pool = &eventName._pool_edit, busday_table = busday_need,
	benchmark_code = &benchmark_code, benchmark_hqinfo = benchmark_hqinfo, my_hqinfo = my_hqinfo, trading_list = &eventName._trading_list, day_detail = &eventName._day_detail);

DATA &eventName._day_detail;
	SET &eventName._day_detail(keep = date n_stock is_holding_benchmark capital last_capital turnover is_trade_day bench_ret accum_alpha ret alpha);
RUN;

/* �������ɵĲ��Խ�����Ϊ: &eventName._day_detail */
/* n_stock: ��Ʊ���� */
/* is_holding_benchmark: 1��ʾ���л�׼ָ�� */
/* capital: ��ĩ�ʽ�*/
/* last_capital: �ճ��ʽ� */
/* turnover: ���ֽ��(˫��) */
/* is_trade_day: 1-��Ҫ����,0-���轻�� */
/* bench_ret: ��׼������ */
/* accum_alpha: �ۻ�alpha */
/* ret: ���������� */
/* alpha: ����alpha */



