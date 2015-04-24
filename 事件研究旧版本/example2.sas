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




/*例子示范:*/
/* 
假设:
(1) 指定逻辑库: 临时逻辑库(work)
(2) 事件名定义为: event (可任意设置)
(3) 事件研究生成的外部文档存储在: D:\Research\Data\output_data 文件夹中
(4) 事件研究可能需要的输入外部文档存储在: D:\Research\Data\input_data 文件夹中
(5) 比较基准为: 沪深300 
*/

/* 第1步:
连接数据库: initial_sas.sas文件 
运行: environment_set.sas 文件
(假设中提到的所有参数，都在该文件的最上方“参数设置”中，请对应修改。一定要保证指定的文件夹存在)
*/

%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\initial_sas.sas";
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\environment_set_2.sas";

/*
第2步:
准备好事件文件(假设命名为: subset, 并且已经放置在指定的逻辑库中，即work.subset)
必须包含的字段有: event_id, date, stock_code
其中: event_id 要求是唯一的

另外: date要求是交易日。
如果不是交易日，请使用date_macro.sas中的adjust_date函数进行调整。
*/

/* 相应的代码为: */
%adjust_date(busday_table = &my_library..busday , raw_table = &my_library..subset ,colname = date); 
DATA &my_library..subset_neat(drop = adj_date);
	SET &my_library..subset(drop = date date_is_busday);;
	date = adj_date;
	FORMAT date mmddyy10.;
RUN;

/* 去重：一只股票，一天只有一条记录 */
/* 这个步骤会影响之后，每天收益率的计算，所以一定要注意！！*/
PROC SORT DATA = &my_library..subset_neat NODUPKEY;
	BY stock_code date;
RUN;


/* 
第3步:事件研究  
假设：
(1) 关注: 事件窗口[-10,60]的单日收益率(start_win = 10, end_win = 60)
(2) 累计收益率: 从事件日(buy_win = 0)中开始计算
(3) alpha统计文件存储在alpha_result.xls中（注意：代码中的写入，要求该文件中的我们欲写入的Sheet不能已经存在，即: daily_alpha/accum_alpha/accum_alpha_after不能已经存在文件中）
*/

%LET start_win = -10;
%LET end_win = 60;
%LET buy_win = 0;

%MACRO gen_module(bm_name, is_group, group_var, filename);

	/* 事件窗口 */
	%gen_overlapped_win(my_library = &my_library, eventName = &eventName., eventName_strategy = subset_neat, 
			my_hqinfo_with_bm = my_hqinfo_with_&bm_name., start_win = &start_win., end_win = &end_win.);
	/* 计算alpha */
	%cal_access_ret(my_library = &my_library, eventName = &eventName., eventName_hq = &eventName._hq,  buy_win = &buy_win.,
		trading_table = trading_table, list_delist_table=list_delist_table);
	/* 分组 */
	%attribute_to_event(my_library = &my_library, alpha_file = &eventName._alpha , eventName_strategy = subset_neat, 
		output_file = &eventName._alpha , stock_sector_mapping = stock_sector_mapping, stock_bk_mapping = stock_bk_mapping);
	/* 统计 */
	%alpha_collect(my_library = &my_library, alpha_file = &eventName._alpha, 
		alpha_var = accum_alpha, is_group = &is_group., group_var = &group_var. ,filename = &filename, sheetname = &group_var._&bm_name.);
%MEND;



/* 模块1: 行业 */
%gen_module(bm_name=bm, is_group=1, group_var=o_name, filename=&eventName._o_name.xls)
%gen_module(bm_name=indus, is_group=1, group_var=o_name, filename=&eventName._o_name.xls)

/* 模块2: 年份 */
%gen_module(bm_name=bm, is_group=1, group_var=year, filename=&eventName._year.xls)
%gen_module(bm_name=indus, is_group=1, group_var=year, filename=&eventName._year.xls)

/* 模块3: 板块 */
%gen_module(bm_name=bm, is_group=1, group_var=bk, filename=&eventName._bk.xls)
%gen_module(bm_name=indus, is_group=1, group_var=bk, filename=&eventName._bk.xls)



/* 最终会在你指定的输出文件夹路径中，生成文件: alpha_result.xls，其中包含三个Sheet, 分别为: daily_alpha/accum_alpha/accum_alpha_after */
/* 若希望对不同窗口的alpha有其他统计方式，可以直接对: &eventName._&start_win._&end_win. 文件进行统计分析等 */

/* 第4步: 构造等权组合 */
/* 假设：
	(1) 准备好文件: cur_event(任意命名，但需要放置在指定的逻辑库中，即: work.cur_event）
		要求: cur_event的结构为
		event_id: 事件id(唯一)
		date：日期
		stock_code
		max_day：最长持有交易日数量
		min_day: 最短持有天数(暂时没有用到)
		ineffective_date: 失效日(持有时间结束，可以指定为某一个日期)
		score: 用于设置权重，等权只需要给每个都设置为1即可
	（2) max_weight: 个股最高权重
*/

%LET max_weight = 0.05;


/* 如果没有特殊要求， 可以在之前的work.subset_neat文件中完善该文件 */
DATA &my_library..cur_event;
	SET &my_library..subset_neat;
	max_day = 60;  /* 持有60个交易日 */
	min_day = .;
	ineffective_date = '31dec2100'd;  /* 设置为未来很远的天数，表示该参数暂时失效 */
	FORMAT ineffective_date mmddyy10.;
	score = 1;
RUN;

/* 删除掉事件日当天价格缺失的记录 */
%filter_event(my_library = &my_library, event_table = cur_event, delist_table = stock_delist_table, output_table = cur_event);

/* 生成股票池 */
%equally_weighted_stock_pool(my_library = &my_library, eventName = &eventName., event_table = cur_event,
	weight_function  = score, busday_table = &my_library..busday, delist_table = stock_delist_table);

/* 标准化权重 */
%norm_weight(my_library = &my_library, stock_pool = &eventName._pool, 
		is_fixed_size = 0, size =0 , stock_pool_norm_weight=&eventName._pool_edit);

/* 设置最大权重 */
%adjust_weight(my_library = &my_library, stock_pool = &eventName._pool_edit, 
	max_weight = &max_weight., is_other_adjust = 0, stock_pool_adjust_weight = &eventName._pool_edit)

DATA &my_library..&eventName._pool_edit;
	SET &my_library..&eventName._pool_edit(drop =  event_day event_id sell_date last_sell_date last_event_id);
RUN;

/* 最终生成的文件存储在: &eventName._pool_eidt中 */
/* is_buy = 1, is_sell = 0: 当天收盘买入 */
/* is_buy = 0, is_sell = 0: 持有 */
/* is_buy = 0, is_sell = 1: 当天收盘卖出 */



/* 第5步:计算每日收益率 */
/* 假设:
	(1) 策略起始和结束日期 
	(2) 若仓位未满，则用指数(benchmark_code)进行替代，同时alpha的基准也选为(benchmark_code) 
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

/* 最终生成的策略结果表格为: &eventName._day_detail */
/* n_stock: 股票数量 */
/* is_holding_benchmark: 1表示持有基准指数 */
/* capital: 日末资金*/
/* last_capital: 日初资金 */
/* turnover: 换手金额(双边) */
/* is_trade_day: 1-需要交易,0-无需交易 */
/* bench_ret: 基准收益率 */
/* accum_alpha: 累积alpha */
/* ret: 单日收益率 */
/* alpha: 单日alpha */



