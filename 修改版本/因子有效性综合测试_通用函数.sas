/** 针对因子有效性的基础函数进行整合，同时考虑因子分析中的输出结果和分析的完整性 */

/*** 函数列表：
(1) merge_multiple_factor_ic_result: 汇总因子ic/coverage的结果，方便Excel查看 
		要求：在运行"因子有效性_通用函数/test_multiple_factor_ic"后才能执行该函数 
(2) test_single_factor_group_ret: 根据因子得分分为N组，构造等权组合，对比三个组合的差异
(3) test_single_cfactor_higher_ret: 连续得分，取排名最高的min(N,half(nstock))，分别构建等权和得分加权组合
(4) test_single_dfactor_higher_ret: 离散得分，取特定得分股票，构建等权组合
(5) test_multiple_factor_group_ret: 多个因子同时操作(2)
(6) test_multiple_factor_higher_ret: 多个因子同时操作(3)或者(4)
(7) merge_factor_3group_result: 将(5)中的结果(要求分3组)进行汇总，以方便Excel查看
(8) merge_factor_higher_result: 将(6)中的结果进行汇总，以方便Excel查看



/*** 模块1：汇总因子ic/coverage的结果，方便Excel查看 */
/** 需要在运行：因子有效性_通用函数/test_multiple_factor_ic后才能执行该函数 */
/** 输入：
(1) factor_table: 因子表。当中涉及到的因子应该是test_multiple_factor_ic中用到的factor_table的子集。
(2) merge_var: 需要是test_multiple_factor_ic的输出结果(&fname._ic或者&fname._cover中出现的变量)，如：
			&fname._ic中的s_ic_f1 或者 &fname._cover中的pct
(3) suffix: cover或者ic中的一者
(4) is_hit: 1-计算胜率(>=0的占比) 0-计算均值
(5) output_table: 输出结果。包含：
	(i) factor: 因子名称
	(ii) &merge_var.:统计的变量
(6) exclude_list: factor_table中不考虑的字段，要求大写
**/

%MACRO merge_multiple_factor_ic_result(factor_table, merge_var, suffix, is_hit, output_table, exclude_list=(''));
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		&merge_var. LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
          PROC SQL;
			CREATE TABLE tmp AS
			SELECT "&fname." AS factor, 
			mean(&merge_var.) AS &merge_var._mean,
			sum(&merge_var.>0)/sum(not missing(&merge_var.)) AS &merge_var._hit
			FROM &fname._&suffix.
		QUIT;
		%IF %SYSEVALF (&is_hit.=0) %THEN %DO;
			DATA &output_table.;
				SET &output_table. tmp(rename = (&merge_var._mean = &merge_var.) drop =&merge_var._hit );
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. tmp(rename = (&merge_var._hit = &merge_var.) drop =&merge_var._mean);
			RUN;
		%END;	
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;

%MEND merge_multiple_factor_ic_result;


/** 模块2: 根据因子得分分为N组，构造等权组合，对比N个组合的差异 */
/** 输入：
(1) factor_table: 因子得分列表
(2) fname: 因子名称
(3) bm_index: 只有当index_result_type=1时才有效。要求: end_date, daily_ret
(4) index_result_type: 1-相对收益 2-绝对收益
(5) adjust_date: 调仓日期表(end_date)
(6) test_date: 组合日期表(date)
(7) start_date: 绩效评估开始日期
(8) end_date: 绩效评估结束日期 (注意：7和8一般就是test_date的起始和结束日期)
(9) annualized_factor: 计算annualized时的factor
(10) ngroup: 分成的N组
(11) is_cut: 1- 分N组(适合连续变量) 0- 适合离散变量(直接利用fname的值)

**/
/** 输出：
&fname._g&rank._index
&fname._g&rank._stat
&fname._g&rank._trade
**/
%MACRO test_single_factor_group_ret(factor_table, fname, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				ngroup =3, is_cut=1);
	DATA &factor_table._c(keep = end_date stock_code &fname.);
		SET &factor_table.;
		IF not missing(&fname.);
	RUN;
	/** 分成N组 */
	%single_factor_score(raw_table=&factor_table._c, identity=stock_code, factor_name=&fname.,
		output_table=&factor_table._c, is_increase = 1, group_num = &ngroup.);
	%IF %SYSEVALF(&is_cut.=1) %THEN %DO;
		%LET test_var = &fname._score;
	%END;
	%ELSE %DO;
		%LET test_var = &fname.;
	%END; 

	PROC SQL NOPRINT;
		SELECT distinct &test_var., count(distinct &test_var.) 
		INTO :test_var_list SEPARATED BY ' ',
			 :ntest_var
		FROM &factor_table._c;
	QUIT;
	
	%DO index = 1 %TO &ntest_var.;
		%LET rank = %scan(&test_var_list., &index., ' ');
		PROC SQL;
			CREATE TABLE &factor_table._c2 AS
			SELECT end_date, stock_code, 1 AS weight
			FROM &factor_table._c
			WHERE &test_var. = &rank.;
		QUIT;
		%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._g&rank._index, 
			output_stat_table=&fname._g&rank._stat, 
			output_trade_table=&fname._g&rank._trade, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);
	%END;
	PROC SQL;
		DROP TABLE &factor_table._c2, &factor_table._c;
	QUIT;
%MEND test_single_factor_group_ret;

/*** 模块3：连续得分，取排名最高的min(N,half(nstock))，分别构建等权和得分加权组合 */
/** 注意：因这里需要根据得分加权构造组合，所以要求得分>0，而对于那些反向的因子，可以构造对应的"负因子"让要求的组合得分>0*/
/** 输入：
(1) factor_table: 因子得分列表
(2) fname: 因子名称
(3) bm_index: 只有当index_result_type=1时才有效。要求: end_date, daily_ret
(4) index_result_type: 1-相对收益 2-绝对收益
(5) adjust_date: 调仓日期表(end_date)
(6) test_date: 组合日期表(date)
(7) start_date: 绩效评估开始日期
(8) end_date: 绩效评估结束日期 (注意：7和8一般就是test_date的起始和结束日期)
(9) annualized_factor: 计算annualized时的factor
(10) nstock: 取多少股票
**/
/** 输出：
&fname._&nstock._index_e/w
&fname._&nstock._stat_e/w
&fname._&nstock._trade_e/w
**/

%MACRO test_single_cfactor_higher_ret(factor_table, fname, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				nstock=100);
	DATA &factor_table._c(keep = end_date stock_code &fname.);
		SET &factor_table.;
		IF not missing(&fname.);
	RUN;
	/** Step1: 取排名最高的前N名(或一半)*/
	%single_factor_score(raw_table=&factor_table._c, identity=stock_code, factor_name=&fname.,
		output_table=&factor_table._c, is_increase = 1, group_num =2);
	%cut_subset(input_table=&factor_table._c, colname=&fname., output_table=&factor_table._c,
					type=3, threshold=&nstock., is_top=1, is_cut=0);

	/** 有一些因为精度问题，看似大于0，但其视为0。之后在得分加权的组合中，其权重实质上为0 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT *
		FROM &factor_table._c
		WHERE cut_mark = 1 AND &fname. > 0 AND abs(&fname.) >= 0.00001 AND 
		&fname. >= (SELECT min(&fname.) FROM &factor_table._c WHERE &fname._score = 2);
	QUIT;
	DATA &factor_table._c;
		SET tmp;
	RUN;

	/** Step2: 等权组合 */
	PROC SQL;
		CREATE TABLE &factor_table._c2 AS
		SELECT end_date, stock_code, 1 AS weight
		FROM &factor_table._c
	QUIT;
	%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._&nstock._index_e,
			output_stat_table=&fname._&nstock._stat_e,
			output_trade_table=&fname._&nstock._trade_e, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);


	/** Step3: 得分加权组合 */
	PROC SQL;
		CREATE TABLE &factor_table._c2 AS
		SELECT end_date, stock_code, &fname. AS weight
		FROM &factor_table._c;
	QUIT;

	%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._&nstock._index_w,
			output_stat_table=&fname._&nstock._stat_w,
			output_trade_table=&fname._&nstock._trade_w, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);
	PROC SQL;
		DROP TABLE &factor_table._c, &factor_table._c2;
	QUIT;

%MEND test_single_cfactor_higher_ret;


/*** 模块4：离散得分，取得分为特定值的变量，构建等权组合 */
/** 这里不要求得分加权，因为得分都是相同的。同时也不用要求得分>0这个条件了*/
/** 输入：
(1) factor_table: 因子得分列表
(2) fname: 因子名称
(3) fname_value: 特定因子值
(3) bm_index: 只有当index_result_type=1时才有效。要求: end_date, daily_ret
(4) index_result_type: 1-相对收益 2-绝对收益
(5) adjust_date: 调仓日期表(end_date)
(6) test_date: 组合日期表(date)
(7) start_date: 绩效评估开始日期
(8) end_date: 绩效评估结束日期 (注意：7和8一般就是test_date的起始和结束日期)
(9) annualized_factor: 计算annualized时的factor
**/
/** 输出：
&fname._&&fname_value._index_e
&fname._&fname_value._stat_e
&fname._&fname_value._trade_e
**/

%MACRO test_single_dfactor_higher_ret(factor_table, fname,fname_value, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor);
	DATA &factor_table._c(keep = end_date stock_code &fname.);
		SET &factor_table.;
		IF not missing(&fname.) AND &fname. = &fname_value.;
	RUN;

	/** Step1: 等权组合 */
	PROC SQL;
		CREATE TABLE &factor_table._c2 AS
		SELECT end_date, stock_code, 1 AS weight
		FROM &factor_table._c
	QUIT;
	%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._&fname_value._index_e,
			output_stat_table=&fname._&fname_value._stat_e,
			output_trade_table=&fname._&fname_value._trade_e, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);

	PROC SQL;
		DROP TABLE &factor_table._c, &factor_table._c2;
	QUIT;

%MEND test_single_dfactor_higher_ret;



/*** 模块6：根据得分分为N组，构建组合，循环进行因子的测试。***/
/** 输入：
(1) factor_table: 因子得分列表
(2) fname: 因子名称
(3) bm_index: 只有当index_result_type=1时才有效。要求: end_date, daily_ret
(4) index_result_type: 1-相对收益 2-绝对收益
(5) adjust_date: 调仓日期表(end_date)
(6) test_date: 组合日期表(date)
(7) start_date: 绩效评估开始日期
(8) end_date: 绩效评估结束日期 (注意：7和8一般就是test_date的起始和结束日期)
(9) annualized_factor: 计算annualized时的factor
(10) ngroup: 分成的N组
(11) is_cut: 1- 分N组(适合连续变量) 0- 适合离散变量(直接利用fname的值)

**/
/** 输出：(每个因子都有）
&fname._g&rank._index
&fname._g&rank._stat
&fname._g&rank._trade
**/
%MACRO 	test_multiple_factor_group_ret(factor_table, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				ngroup =3, is_cut=1,
				exclude_list=(''));
	PROC CONTENTS DATA = &factor_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
            
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
		  %test_single_factor_group_ret(factor_table=&factor_table., fname=&fname., 
				bm_index=&bm_index., index_result_type=&index_result_type.,
				adjust_date=&adjust_date., test_date=&test_date.,
				start_date=&start_date., end_date=&end_date., 
				annualized_factor=&annualized_factor.,
				ngroup =&ngroup., is_cut=&is_cut.);
     %END;
	PROC SQL;
		DROP TABLE tt_varlist2;
	QUIT;
%MEND test_multiple_factor_group_ret;


/*** 模块7：针对前topN组合(连续）或者特定因子值(离散)构建组合，循环进行因子的测试。***/
/** 注意：这里可以将test_single_cfactor_higher_ret和test_single_dfactor_higher_ret合并一起。
		前者适合连续变量，后者适合离散变量。两个函数除了前者有参数nstock，后者用factor_name外，参数都一样
**/
/** 输入：
(1) factor_table: 因子得分列表
(2) fname: 因子名称
(3) bm_index: 只有当index_result_type=1时才有效。要求: end_date, daily_ret
(4) index_result_type: 1-相对收益 2-绝对收益
(5) adjust_date: 调仓日期表(end_date)
(6) test_date: 组合日期表(date)
(7) start_date: 绩效评估开始日期
(8) end_date: 绩效评估结束日期 (注意：7和8一般就是test_date的起始和结束日期)
(9) annualized_factor: 计算annualized时的factor
(10) var：当is_continuous=1时候var对应nstock, 当is_continuous=0时var对应&factor_name.
(11) is_continuous: 1-连续变量 0-离散变量
(12) exclude_list
**/
/** 输出：
&fname._&nstock._index_e/w
&fname._&nstock._stat_e/w
&fname._&nstock._trade_e/w
**/

%MACRO 	test_multiple_factor_higher_ret(factor_table,bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				var,
				is_continuous=1,
				exclude_list=(''));
	PROC CONTENTS DATA = &factor_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
	 %IF %SYSEVALF(&is_continuous.=1) %THEN %DO;        
    	 %DO i = 1 %TO &nfactors2.;
          	%LET fname =  %scan(&name_list2.,&i., ' ');
		  	%put testing &fname....;
		  	%test_single_cfactor_higher_ret(factor_table=&factor_table., fname=&fname., 
				bm_index=&bm_index., index_result_type=&index_result_type.,
				adjust_date=&adjust_date., test_date=&test_date.,
				start_date=&start_date., end_date=&end_date., 
				annualized_factor=&annualized_factor.,
				nstock=&var.);
     	%END;
	%END;
	%ELSE %IF %SYSEVALF(&is_continuous.=0) %THEN %DO;        
    	 %DO i = 1 %TO &nfactors2.;
          	%LET fname =  %scan(&name_list2.,&i., ' ');
		  	%put testing &fname....;
		  	%test_single_dfactor_higher_ret(factor_table=&factor_table., fname=&fname., 
				fname_value =&var.,
				bm_index=&bm_index., index_result_type=&index_result_type.,
				adjust_date=&adjust_date., test_date=&test_date.,
				start_date=&start_date., end_date=&end_date., 
				annualized_factor=&annualized_factor.);
     	%END;
	%END;

	PROC SQL;
		DROP TABLE tt_varlist2;
	QUIT;
%MEND test_multiple_factor_higher_ret;


/*** 模块8：汇总所有因子分3组的结果，方便Excel查看 */
/** 该函数更适合于连续变量。离散变量没办法估计取值是否一定是1/2/3，组别也无法确定*/
/** 需要在运行：:test_multiple_factor_group_ret后才能执行该函数 */
/** 输入：
(1) factor_table: 因子表。当中涉及到的因子应该是test_multiple_factor_ic中用到的factor_table的子集。
(2) merge_var: 需要是test_multiple_factor_group_ret的输出结果,即&fname._g&group._stat中的变量：
			如：accum_ret/sd/ir/hit_ratio/index_draw/hit_ratio_m/turnover_uni/nstock都可以
(3) output_table: 输出结果。包含：
	(i) factor: 因子名称
	(ii) g1_&merge_var.:第一组统计的变量
	(iii) g2_&merge_var: 第二组
	(iv) g3_&merge_var.: 第三组
(4) exclude_list: factor_table中不考虑的字段，要求大写
**/

%MACRO merge_factor_3group_result(factor_table, merge_var, output_table, exclude_list=(''));
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		g1 LENGTH = 8
		g2 LENGTH = 8
		g3 LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
          PROC SQL;
			CREATE TABLE tmp AS
			SELECT "&fname." AS factor, 
			A.&merge_var. AS g1,
			B.&merge_var. AS g2,
			C.&merge_var. AS g3
			FROM &fname._g1_stat A 
			LEFT JOIN &fname._g2_stat B
			ON A.year = B.year
			LEFT JOIN &fname._g3_stat C
			ON A.year = C.year
			WHERE A.year = 0;
		QUIT;
		DATA &output_table.;
			SET &output_table. tmp;
		RUN;
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;
%MEND merge_factor_3group_result;

/*** 模块9：按年度汇总topN组合的结果，方便Excel查看 */
/** 需要在运行：:test_multiple_factor_higher_ret后才能执行该函数 */
/** 输入：
(1) factor_table: 因子表。当中涉及到的因子应该是test_multiple_factor_ic中用到的factor_table的子集。
(2) suffix: 表名为 &fname._&suffix. 是test_multiple_factor_high_ret的结果
(2) merge_var: 需要是test_multiple_factor_group_ret的输出结果,即&fname._g&group._stat中的变量：
			如：accum_ret/sd/ir/hit_ratio/index_draw/hit_ratio_m/turnover_uni/nstock都可以
(3) output_table: 输出结果。包含：
	(i) factor: 因子名称
	(ii) &merge_var.:第一组统计的变量
(4) exclude_list: factor_table中不考虑的字段，要求大写

**/

/*** 汇总所有因子取前N名的结果，方便Excel查看 */
%MACRO merge_result_higher_total(factor_table, suffix, merge_var, output_table, exclude_list);
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
      
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
		  %put &merge_var.;
		  DATA tmp;
		  	SET &fname._&suffix.(keep = year &merge_var.);
			LENGTH factor $ 30.;
			factor = "&fname.";
		  RUN;
		 PROC TRANSPOSE DATA = tmp OUT = tmp(drop = _NAME_) prefix = Y;
		 	BY factor;
			ID year;
			VAR &merge_var.;
		RUN;
		%IF %SYSEVALF(&i.=1) %THEN %DO;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. tmp;
			RUN;
		%END;
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2, tmp;
	QUIT;

%MEND merge_result_higher_total;

