/* 3�ֲ��ԣ�
(1) �������˳��ź�
(2) �̶���������
(3) �����ź�+��������
(4) �Ʊ��������˳�
*/

/*%INCLUDE  "D:\Research\DATA\yjyg_link\code_20140827\ȫҵ�����¼�����_2.sas";*/

%map_date_to_index(busday_table=busday, raw_table=merge_signal, date_col_name=date, raw_table_edit=merge_signal);
%map_date_to_index(busday_table=busday, raw_table=merge_stock_pool, date_col_name=date, raw_table_edit=merge_stock_pool);

PROC SQL;
	CREATE TABLE merge_stock_pool_2 AS
	SELECT A.*, B.signal, B.tar_cmp, B.date AS signal_date, B.date_index AS index_signal_date
	FROM merge_stock_pool A LEFT JOIN merge_signal B
	ON A.stock_code = B.stock_code AND A.date = B.date
	ORDER BY A.stock_code, A.date;
QUIT;


/* !!!!!!!!!!!!!!! ��Ҫ�����е��źŶ�����Ϊ��ĩ������ģ���Ҫ�趨Ϊ�ճ��ġ�(�ò�����δ���У� ****/
DATA merge_stock_pool_2(drop= r_hold r_hold2 r_hold3 r_hold4 r_signal_date r_index_signal_date r_hold3 r_signal_date3 r_index_signal_date3 dif_day dif_day3);
	SET merge_stock_pool_2;
	BY stock_code;
	RETAIN r_hold 0; 
	RETAIN r_hold2 0;  /* ���е��� */
	RETAIN r_hold3 0; 
	RETAIN r_hold4 0;
	RETAIN r_signal_date .;
	RETAIN r_index_signal_date .;
	RETAIN r_signal_date3 .;
	RETAIN r_index_signal_date3 .;


	IF first.stock_code THEN DO;
		r_hold = 0;
		r_hold2 = 0;
		r_hold3 = 0;
		r_hold4 = 0;
		r_signal_date = .;
		r_index_signal_date = .;
		r_signal_date3 = .;
		r_index_signal_date3 = .;
	END;
	
	/** ��һ�ֲ��ԣ����е������ź� */
	/* ����������ź� */
	hold = r_hold;  /* ��������źŹ�����������ź� */
	IF  tar_cmp = 1 AND signal = 1 THEN r_hold = 1;  /* �����źŷ���������һ�������տ��̺����� */
*	ELSE IF tar_cmp = 1 AND signal = 0 THEN r_hold = 0;
*	ELSE IF tar_cmp = 0 AND signal = 0 THEN r_hold = 0;  /* tar_cmp = 0 and signal = 1 �źŲ��� */
	ELSE IF r_hold = 1 AND signal = 0 THEN r_hold = 0;  /* �����ź� */
	

	/* �ڶ��ֲ��ԣ����й̶����� */
	hold2 = r_hold2;
	IF  tar_cmp = 1 AND signal = 1 THEN DO;
		r_hold2 = 1;
		r_signal_date = signal_date;
		r_index_signal_date = index_signal_date;
	END;
	ELSE DO;
*		IF NOT missing(r_index_signal_date) AND date_index - r_index_signal_date < 60 THEN r_hold2 = 1;
		IF r_hold2 = 1 AND date_index - r_index_signal_date >= 60 THEN  r_hold2 = 0;
	END;
	dif_day = date_index - r_index_signal_date;
	FORMAT r_signal_date mmddyy10.;
	

	/* �����ֲ���: �����ź�+�����ʱ�� */
	hold3 = r_hold3;
	IF  tar_cmp = 1 AND signal = 1 THEN DO;
		r_hold3 = 1;
		r_signal_date3 = signal_date;
		r_index_signal_date3 = index_signal_date;
	END;
	ELSE DO;
		IF r_hold3 = 1 AND signal = 0 THEN r_hold3 = 0;
		ELSE IF r_hold3 = 1 AND date_index - r_index_signal_date3 >= 60 THEN  r_hold3 = 0;
	END;
	dif_day3 = date_index - r_index_signal_date3;
	FORMAT r_signal_date3 mmddyy10.;

	/* �����ֲ���: �Ʊ����������� */
	hold4 = r_hold4;
	IF tar_cmp = 1 AND signal = 1 THEN r_hold4 = 1;  /* �����źŷ���������һ�������տ��̺����� */
	ELSE IF r_hold4 = 1 AND (signal = 0 OR tar_cmp = 0) THEN r_hold4 = 0;  /* �����ź� */
	
RUN;

DATA merge_stock_pool;
	SET merge_stock_pool_2;
RUN;


/* ��ʱ���账�� */
/* �����޷�����/��������� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.close, B.open, B.pre_close, B.high, B.low, B.vol
	FROM merge_stock_pool_2 A LEFT JOIN hqinfo B
	ON A.stock_code = B.stock_code AND A.date = B.end_date
	ORDER BY A.stock_code, A.date;
QUIT;

DATA merge_stock_pool(drop = close open pre_close high low vol);
	SET tmp;
	IF date = list_date THEN not_trade = 1; /* �������գ��������� */
	IF missing(vol) OR vol = 0 THEN not_trade = 1;
	ELSE IF close = high AND close = low AND close = open AND close > pre_close THEN not_trade = 1;  /* ��ͣ */
	ELSE IF close = high AND close = low AND close = open AND close < pre_close THEN not_trade = 1; /* ��ͣ */
	ELSE not_trade = 0;
RUN;



DATA merge_stock_pool(drop = rr_hold rr_hold2 rr_hold3 rr_hold4 list_date delist_date start_date end_date signal_date index_signal_date);
	SET merge_stock_pool;
	BY stock_code;
	RETAIN rr_hold .;
	RETAIN rr_hold2 .;
	RETAIN rr_hold3 .;
	RETAIN rr_hold4 .;
	IF first.stock_code THEN DO;
		rr_hold = .;
		rr_hold2 = .;
		rr_hold3 = .;
		rr_hold4 = .;
	END;
	IF (rr_hold = 0 OR missing(rr_hold)) AND hold = 1 THEN DO ;   /* �״����� */
		IF not_trade = 1 THEN DO;
			f_hold = 0;  /* �޷����� */
		END;
		ELSE DO;
			f_hold = 1;
		END;
	END;
	/* ��Ҫ���� */
	ELSE IF rr_hold = 1 AND hold = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold = 1;  
		END;
		ELSE DO;
			f_hold = 0;
		END;
	END;
	ELSE DO;
		f_hold = hold;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold = 0; /* ����Ѿ������һ�������գ���ǡ���������գ���ǿ������ */
	rr_hold = f_hold;

	/* ���е��ڲ��� */
	IF (rr_hold2 = 0 OR missing(rr_hold2)) AND hold2 = 1 THEN DO ;   /* �״����� */
		IF not_trade = 1 THEN DO;
			f_hold2 = 0;  /* �޷����� */
		END;
		ELSE DO;
			f_hold2 = 1;
		END;
	END;
	/* ��Ҫ���� */
	ELSE IF rr_hold2 = 1 AND hold2 = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold2 = 1;  
		END;
		ELSE DO;
			f_hold2 = 0;
		END;
	END;
	ELSE DO;
		f_hold2 = hold2;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold2 = 0; /* ����Ѿ������һ�������գ���ǡ���������գ���ǿ������ */
	rr_hold2 = f_hold2;

	/* �����ź�+�������� */
	IF (rr_hold3 = 0 OR missing(rr_hold3)) AND hold3 = 1 THEN DO ;   /* �״����� */
		IF not_trade = 1 THEN DO;
			f_hold3 = 0;  /* �޷����� */
		END;
		ELSE DO;
			f_hold3 = 1;
		END;
	END;
	/* ��Ҫ���� */
	ELSE IF rr_hold3 = 1 AND hold3 = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold3 = 1;  
		END;
		ELSE DO;
			f_hold3 = 0;
		END;
	END;
	ELSE DO;
		f_hold3 = hold3;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold3 = 0; /* ����Ѿ������һ�������գ���ǡ���������գ���ǿ������ */
	rr_hold3 = f_hold3;

	/* ���ǲƱ����� */
	IF (rr_hold4 = 0 OR missing(rr_hold4)) AND hold4 = 1 THEN DO ;   /* �״����� */
		IF not_trade = 1 THEN DO;
			f_hold4 = 0;  /* �޷����� */
		END;
		ELSE DO;
			f_hold4 = 1;
		END;
	END;
	/* ��Ҫ���� */
	ELSE IF rr_hold4 = 1 AND hold4 = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold4 = 1;  
		END;
		ELSE DO;
			f_hold4 = 0;
		END;
	END;
	ELSE DO;
		f_hold4 = hold4;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold4 = 0; /* ����Ѿ������һ�������գ���ǡ���������գ���ǿ������ */
	rr_hold4 = f_hold4;
RUN;
