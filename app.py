"""
    「kubun.」は？
    print(
        Query.From(Query.From("shohin").lessThan("price", 250))
                .leftJoin(Query.From("kubun").lessThan("kubun_id", 3), "kubun_id")
    )
    |shohin.shohin_id|shohin.shohin_name|shohin.kubun_id|shohin.price|kubun_id|kubun_name|
    |2|みかん|1|130|1|くだもの|
    |3|キャベツ|2|200|2|野菜|
    |4|さんま|3|220|None|None|
    |6|しいたけ|2|180|2|野菜|

    kubun_id=Noneは？
    print(
        shohin.groupBy("kubun_id", Count("shohin_name"), Average("price"))
    )
    |kubun_id|count|average|
    |1|2|215.0|
    |2|2|190.0|
    |3|1|220.0|
"""

import copy # copy.deepcopy(the_tuple)
from abc import ABCMeta, abstractmethod # class AbstractClass(metaclass=ABCMeta):

def main():
	# テーブル作成
	# 商品マスタ
    shohin = Table.create( "shohin", ["shohin_id", "shohin_name", "kubun_id", "price"])
    shohin.insert(1, "りんご", 1, 300)\
        .insert(2, "みかん", 1, 130)\
        .insert(3, "キャベツ", 2, 200)\
        .insert(4, "さんま", 3, 220)\
        .insert(5, "わかめ", None, 250)\
        .insert(6, "しいたけ", 2, 180)

	# 商品区分マスタ
    kubun = Table.create("kubun", ["kubun_id", "kubun_name"])
    kubun.insert(1, "くだもの").insert(2, "野菜").insert(3, "魚")

    # クエリ（select）
    print('全件')
    print(shohin)
    print(Query.From("shohin"))
    print(Query.From(shohin))
    print(Query.From(Query.From("shohin")))

    print('射影')
    print(Query.From("shohin").select("shohin_name", "price"))

    print('選択')
    print(Query.From("shohin").lessThan("price", 250))

    print('結合')
    print(Query.From("shohin").leftJoin("kubun", "kubun_id"))

    print('結合＋選択＋射影')
    print(Query.From("shohin").leftJoin("kubun", "kubun_id")
                    .lessThan("price", 200).select("shohin_name", "kubun_name", "price"))

    print('サブクエリ')
    print(Query.From(Query.From("shohin").lessThan("price", 250))
                .leftJoin(Query.From("kubun").lessThan("kubun_id", 3), "kubun_id"))

    print('演算（＝）')
    print(Query
        .From("shohin")
        .leftJoin("kubun", "kubun_id")
        .equals("shohin_id", 2)
        .select("shohin_id", "shohin_name", "kubun_name", "price"))
    
    # ORDER BY
    print(Query.From(shohin))
    print(Query.From(shohin).orderBy('price'))
    print(shohin.orderBy('price', False))
    print(shohin.orderBy('kubun_id'))

    # GROUP BY
    print(
        shohin.groupBy("kubun_id", Count("shohin_name"), Average("price"))
    )

    # 演算（＜）
    print(
        Query.From(Query.From("shohin").lessThan("price", 250))
        .leftJoin(Query.From("kubun").lessThan("kubun_id", 3), "kubun_id")
    )
    print(
        Query
          .From("shohin")
          .leftJoin("kubun", "kubun_id")
          .lessThan("price", 1000)
          .select("shohin_name", "kubun_name", "price")
    )


# テーブルの一覧
tables = {}

# リレーション（基底クラス）
class Relation:
    columns = []
    tuples = []

    def __init__(self, columns, tuples):
        self.columns = columns
        self.tuples = tuples

    # カラムを探す
    def findColumn(self, name):
        for i in range(0, len(self.columns)):
            if self.columns[i].name == name:
                return i
        return len(columns)

    # 簡易整形
    # |shohin_id|shohin_name|kubun_id|price|
    # |1|りんご|1|300|
    # |2|みかん|1|130|
    # |3|キャベツ|2|200|
    def __str__(self):
        buf = ''
        # フィールド名
        for cl in self.columns:
            buf += "|"
            buf += cl.parent + "." if cl.parent != '' else ''
            buf += cl.name
        buf += "|" + '\n'
        # データ
        for t in self.tuples:
            for v in t.values:
                buf += "|"
                buf += str(v)
            buf += "|" + '\n'
        return buf

    def select(self, *columnNames):
        indexes = []
        newColumns = []
        for n in columnNames:
            newColumns.append(Column(n))
            idx = self.findColumn(n)
            indexes.append(idx)
        # データの投影
        newTuples = []
        for tp in self.tuples:
            values = []
            for idx in indexes:
                if idx < len(tp.values):
                    values.append(tp.values[idx])
                else:
                    values.append(None)
            newTuples.append(Tuple(values))        
        return Query(newColumns, newTuples)
    
    def leftJoin(self, tableNameOrRelation, matchingField):
        tbl = Query.From(tableNameOrRelation)
        # 属性の作成
        newColumns = copy.deepcopy(self.columns)
        for cl in tbl.columns:
            newColumns.append(Column(cl.name))
        
        # 値の作成
        newTuples = []
        leftColumnIdx = self.findColumn(matchingField)
        rightColumnIdx = tbl.findColumn(matchingField)
        # 該当フィールドがない場合は結合しない
        if leftColumnIdx >= len(self.columns) or rightColumnIdx >= len(tbl.columns):
            return Query(newColumns, [])

        # 結合処理
        for tp in self.tuples:
            # 元のテーブルのデータ
            ntpl = Tuple(copy.deepcopy(list(tp.values)))
            # 足りないフィールドを埋める
            while(len(ntpl.values) < len(self.columns)):
                ntpl.values.append(None)
            # 結合対象のフィールドを探す
            leftValue = ntpl.values[leftColumnIdx]
            # 一致するタプルを抽出
            leftRel = tbl.equals(matchingField, leftValue)
            # 一致するタプルがあれば結合
            if leftRel.tuples:
                # 今回は、タプルの対応は一対一まで
                for v in leftRel.tuples[0].values:
                    ntpl.values.append(v)
                        
            # 足りないフィールドを埋める
            while(len(ntpl.values) < len(newColumns)):
                ntpl.values.append(None)
            
            newTuples.append(ntpl)
        return Query(newColumns, newTuples)
    
    def lessThan(self, columnName, value):
        idx = self.findColumn(columnName)
        if idx >= len(self.columns):
            return Query(columns, [])
        newTuples = []
        for tp in self.tuples:
            if tp.values[idx] < value:
                newTuples.append(tp)
        return Query(self.columns, newTuples)

    def equals(self, columnName, value):
        if value == None:
            return Query(self.columns, [])
        idx = self.findColumn(columnName)
        if idx >= len(self.columns):
            return Query(self.columns, [])
        newTuples = []
        for tp in self.tuples:
            if value == tp.values[idx]:
                newTuples.append(tp)
        return Query(self.columns, newTuples)

    def orderBy(self, columnName, asc = True):
        idx = self.findColumn(columnName)
        if idx >= len(self.columns):
            return self
        newTuple = copy.deepcopy(self.tuples)
        # 整列ルールにNone対応タプルを使う
        sortedTuple = sorted(newTuple, key=lambda t: (t.values[idx] is None, t.values[idx]), reverse = False if asc else True)
        return Relation(self.columns, sortedTuple)

    def groupBy(self, columnName, *aggregations):
        # 列名を作成
        newColumns = []
        newColumns.append(Column(columnName))
        colIndexes = []
        for agg in aggregations:
            newColumns.append(Column(agg.getName()))
            colIndexes.append(self.findColumn(agg.columnName))
        
        # 集計行を取得
        idx = self.findColumn(columnName)
        if idx >= len(self.columns):
            return Relation(newColumns, [])
        
        # あらかじめソート
        sorted = self.orderBy(columnName)
        
        current = None
        newTuples = []
        for tp in sorted.tuples:
            # 集計フィールド取得
            if len(tp.values) <= idx:
                continue
            v = tp.values[idx]
            if v == None:
                continue
            if v != current:
                if current != None:
                    # 集計行を追加
                    values = []
                    values.append(current)
                    for agg in aggregations:
                        values.append(agg.getResult())
                    newTuples.append(Tuple(values))
                current = v
                for agg in aggregations:
                    agg.reset()
            # 集計
            for i in range(0, len(aggregations)):
                aidx = colIndexes[i]
                if len(tp.values) <= aidx:
                    continue
                cv = tp.values[aidx]
                if cv == None:
                    continue
                ag = aggregations[i]
                ag.addData(cv)
        if current != None:
            # 集計行を追加
            values = []
            values.append(current)
            for agg in aggregations:
                values.append(agg.getResult())
            newTuples.append(Tuple(values))
        
        return Relation(newColumns, newTuples)

# クエリ
class Query(Relation):
    @classmethod
    def From(cls, tableNameOrRelation):
        if isinstance(tableNameOrRelation, str):
            t = tables[tableNameOrRelation]
            newColumns = []
            for cl in t.columns:
                newColumns.append(Column(tableNameOrRelation, cl.name))
            return Query(newColumns, t.tuples)
        elif isinstance(tableNameOrRelation, Relation):
            return tableNameOrRelation

# テーブル
class Table(Relation):
    name = ''

    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.tuples = []

    @classmethod
    def create(cls, name, columnNames):
        columns = []
        for n in columnNames:
            columns.append(Column(n))
        t = Table(name, columns)
        tables[name] = t
        return t
    
    def insert(self, *args):
        self.tuples.append(Tuple(args))
        return self

# 属性値
class Tuple:
    values = []
    def __init__(self, values):
        self.values = values

# カラム
class Column:
    parent = ''
    name = ''
    def __init__(self, *args):
        if len(args) == 1:
            self.parent = ''
            self.name = args[0]
        else:
            self.parent = args[0]
            self.name = args[1]

# 集計用ベースクラス
class Aggregation(metaclass=ABCMeta):
    columnName = ''
    
    def __init__(self, columnName):
        self.columnName = columnName

    # 関数名（デフォルトの列名）
    @abstractmethod
    def getName(self):
        pass

    # データ追加
    @abstractmethod
    def addData(self, value):
        pass

    # 結果取得
    @abstractmethod
    def getResult(self):
        pass

    # リセット
    @abstractmethod
    def reset(self):
        pass

# 集計（カウント）
class Count(Aggregation):
    counter = 0

    def __init__ (self, columnName):
        super().__init__(columnName)

    def getName(self):
        return "count"

    def addData(self, value):
        self.counter += 1

    def getResult(self):
        return self.counter

    def reset(self):
        self.counter = 0

# 集計（平均）
class Average(Aggregation):
    counter = 0
    total = 0

    def __init__ (self, columnName):
        super().__init__(columnName)

    def getName(self):
        return "average"

    def addData(self, value):
        self.counter += 1
        self.total += value

    def getResult(self):
        return self.total / self.counter

    def reset(self):
        self.counter = 0
        self.total = 0

if __name__ == "__main__":
    main()
