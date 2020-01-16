import traceback
import sys

import re
import requests

import MeCab
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext


def aggregate_tags_count(new_values, total_sum):
    '''
    ワードカウントの更新
    '''
    return sum(new_values) + (total_sum or 0)

def mecabb(text):
    '''
    ツイートを形態素解析
    '''
    words = []
    try:
        # 外で生成したmecabを使うと落ちる。中で生成する必要あり。
        # TODO MeCabをグローバル変数にして外に出せば解決するか検証
        mecab = MeCab.Tagger("-Ochasen")
        mecab.parse("")
        m = mecab.parseToNode(text)

        while m:
            # 名詞だけ抽出
            if m.feature.split(",")[0] == "名詞":
                # 日本語以外削除
                word = re.sub("[^ぁ-んァ-ンー一-龠]", "", m.surface)
                # 空文字はリストに入れない             
                if len(m.surface) != 0:
                    words.append(word)
            m = m.next
        print(words)
        return words
    except Exception as e:
        print(e)


def get_sql_context_instance(spark_context):
    '''
    SQLコンテキスト存在チェック
    '''
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    '''
    集計結果の表示
    '''
    print(f"----------- {str(time)} -----------")
    try:
        # SQLコンテキスト生成
        sql_context = get_sql_context_instance(rdd.context)
        # ROWオブジェクト生成
        row_rdd = rdd.map(lambda w: Row(word=w[0], count=w[1]))
        # DataFrame生成
        df = sql_context.createDataFrame(row_rdd)
        # テーブル登録
        df.registerTempTable("word_count_table")
        # SQL実行
        df = sql_context.sql("""
            select 
              word 
             ,count 
            from 
              word_count_table 
            where 
              word != '' and word is not null
            order by 
              count desc
            """)
        # 集計結果表示
        df.show()
    except:
        # traceback.print_exc()
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# SparkContextの生成
sc = SparkContext(conf=SparkConf().setAppName("TwitterStreamApp"))
sc.setLogLevel("ERROR")

# バッチ起動インターバルを1秒に設定
ssc = StreamingContext(sc, 3)

# チェックポイントを生成
ssc.checkpoint("checkpoint_TwitterApp")

# ソケットからデータを取得
dataStream = ssc.socketTextStream("000.000.00.0",5555)

# ツイートを形態素解析して集計
count = dataStream.flatMap(mecabb).map(lambda x: (x, 1))

# ワードカウントの更新
tags_totals = count.updateStateByKey(aggregate_tags_count)

# 集計結果の表示
# 何か遅い...
# sparkの外で前処理した方がいい
tags_totals.foreachRDD(process_rdd)

# 処理の開始
ssc.start()

# 動作を停止させるコマンドを待つ 
# → Ctrl + Cとか
ssc.awaitTermination()