<?php
/**
 * mongoDB php7兼容库文件
 * author <xu_hao@loongyjoy.com>
 * time 2018-09-07
 */
use MongoDB\BSON\ObjectId;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;

/**
 * Class Mongo
 * Mongo单例
 */
class Mongo {

    private static $instance;

    public static function init()
    {
        if(!self::$instance){
            self::$instance = new Mongo();
        }
        return self::$instance;
    }

    private function __construct()
    {
    }

    function __get($name)
    {
        $this->$name = mongoDataBase::init($name);
        return $this->$name;
    }

    public function connect()
    {
        return true;
    }
}

/**
 * Class mongoDataBase
 * MongoDB类，可由多个库生成多个实例
 */
class mongoDataBase {
    private static $database = [];
    public $_conn          = null;
    public $_db            = null;

    protected $readPreference = NULL;//数据读取模式
    private $currentCursor = null;

    public $filter = array(); //过滤字符串
    public $queryOptions = array(); //查询选项
    public $projection = array();

    public $_coll;  //集合名
    public $skip = 0;
    public $limit = 0;
    public $sort = array();

    private static $default = [
        'default' => MONGODB_DB,
    ];

    public static function init( $dbName )
    {
        if ( !$dbName ) {
            $dbName = self::$default['default'];
        }
        if (!isset(self::$database[$dbName])) {
            $m = new mongoDataBase($dbName);
            self::$database[$dbName] = $m;
        }
        return self::$database[$dbName];
    }

    private function __construct( $dbName )
    {
        $this->_conn = new Manager('mongodb://'.MONGODB_HOST.':'.MONGODB_PORT."/{$dbName}");
        $this->_db   = $dbName;
    }

    function __get($name)
    {
        $this->_coll = $name;
        return $this;
    }

    function connect()
    {
        return true;
    }

    /**
     * @param $array
     * @return array
     * 对象转数组
     */
    function object_array($array) {
        if(is_object($array)) {
            $array = (array)$array;
        } if(is_array($array)) {
            foreach($array as $key=>$value) {
                if(!is_a($value,'MongoDB\BSON\ObjectId')){
                    $array[$key] = $this->object_array($value);
                }
            }
        }
        return $array;
    }

    /**
     * 格式化id数据类型
     * @param $id
     * @return int|ObjectId|string
     */
    public function format($id){
        if(is_int($id)){
            return intval($id);
        }elseif(is_object($id)){
            return $id;
        }else{
            if(strlen($id)<=24){
                $id = sprintf("%024s",$id);
                return  new ObjectId($id);
            }else{
                return $id;
            }
        }
    }

    function handle_document(&$document){

        foreach($document as $doc_name=>$doc){

            if(is_array($doc)){
                $this->handle_document($document[$doc_name]);
            }


            if(strpos($doc_name,'.')>0){
                $doc_arr = explode('.',$doc_name);
                unset($document[$doc_name]);
                $doc_arr[0] = str_replace('$','',$doc_arr[0]);
                $doc_arr[1] = str_replace('$','',$doc_arr[1]);
                $document[$doc_arr[0]][$doc_arr[1]] = $doc;

                if(is_array($doc)){
                    $this->handle_document($document[$doc_arr[0]][$doc_arr[1]]);
                }
            }

            if(preg_match('/\$/',$doc_name)){
                $document[str_replace('$','',$doc_name)] = $doc;
                unset($document[$doc_name]);

            }

        }
    }

    /**
     * 获取当前mongoDB Manager
     * @return MongoDB\Driver\Manager
     */
    function getMongoManager()
    {
        return $this->_conn;
    }

    public function count($filter=[]){
        return $this->countMany($filter);
    }

    /**
     * @param array $filter
     * @param array $projection
     * @param bool $flag
     * @return mongoCursor
     * 查询多条数据
     */
    function find($filter=array(), array $projection = [], $flag=true)
    {
        $this->filter = $filter;
        $this->projection = $projection;
        return new mongoCursor($this, $flag);
    }

    /**
     * 插入一条数据
     * @param $a
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function insert(&$a, array $options = array()) {
        $result = $this->insertOne($a,$options);
        $return = $result->getInsertedCount() ? true : false;
        return $return;
    }

    /**
     * Update 操作
     * @param array $criteria
     * @param array $newobj
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function update(array $criteria , array $newobj, array $options = array()) {
        $upsert = $options['upsert'] ? true : false;
        unset($options['upsert']);
        $multiple =  $options['multiple'] ? true : false;
        unset($options['multiple']);
        $result = $this->updateOperation($criteria, $newobj, $upsert, $options, null, $multiple);
        $return = $result->getInsertedCount() || $result->getModifiedCount() || $result->getMatchedCount() ? true : false;
        return $return;
    }

    /**
     * save 操作
     * @param $a
     * @param array $options
     * @return array|\MongoDB\Driver\WriteResult
     */
    public function save(&$a) {
        if(isset($a['_id'])){
            $data = $a;
            unset($data['_id']);
            return $this->update(array('_id'=>$a['_id']),$data);
        }
        return $this->insert($a);
    }

    /**
     * 删除数据
     * @param array $criteria
     * @param array $options
     * @return \MongoDB\Driver\WriteResult
     */
    public function remove(array $criteria = array(), array $options = array()) {
        $limit = $options['justOne'] ? true : false;
        unset($options['justOne']);
        $result = $this->delete($criteria, $options, null, $limit);
        $return = $result->getDeletedCount() ? true : false;
        return $return;
    }

    /**
     * 插入一个文档
     * @param $document
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function insertOne(&$document, array $options = [],WriteConcern $writeConcern = null) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $this->handle_document($document);
        $insertId = $bulk->insert($document);
        if(!empty($insertId)) $document['_id'] = $insertId;
        $collectionName = $this->_coll;
        $databaseName =  $this->_db;
        $writeResult  = $this->_conn->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        //$insertedCount = $result->getInsertedCount();//返回插入的文档数量
        return $writeResult;
    }

    /**
     * 插入多个文档
     * @param $documents
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function insertMany(array &$documents,array $options = [],WriteConcern $writeConcern = null) {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        foreach ($documents as &$document){
            $insertId = $bulk->insert($document);
            if(!empty($insertId)) $document['_id'] = $insertId;
        }
        $collectionName = $this->_coll;
        $databaseName =  $this->_db;
        $writeResult  = $this->_conn->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 返回数量
     * @param array $filter
     * @param ReadPreference|NULL $readPreference
     * @return int
     * @throws \Exception
     */
    public function countMany($filter = [], ReadPreference $readPreference = null){
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }
        $cmd = ['count' => $this->_coll];
        $cmd['query'] = (object) $filter;
        $command = new Command($cmd);//mongodb各种命令详解：https://docs.mongodb.com/manual/reference/command/nav-crud/
        $cursor = $this->_conn->executeCommand($this->_db, $command, $readPreference);
        $result = current($cursor->toArray());
        // Older server versions may return a float
        if ( ! isset($result->n) || ! (is_integer($result->n) || is_float($result->n))) {
            throw new \Exception('count command did not return a numeric "n" value');
        }
        return (integer) $result->n;
    }

    /**
     * 查询方法
     * @param $filter
     * @param array $queryOptions
     * @param ReadPreference|NULL $readPreference
     * @return \MongoDB\Driver\Cursor
     */
    public function query($filter, array $queryOptions = [], ReadPreference $readPreference = null){
        $query = new Query($filter, $queryOptions);
        if(empty($readPreference)){
            $readPreference = $this->readPreference;
        }
        $collectionName = $this->_coll;
        $databaseName = $this->_db;
        $cursor = $this->_conn->executeQuery($databaseName . '.' . $collectionName, $query, $readPreference);
        return $cursor;
    }

    /**
     * 修改一个文档
     * @param $filter
     * @param $newObj
     * @param bool $upsert  是否在没有匹配到修改文档时，插入数据。
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function updateOne($filter, $newObj ,bool $upsert = false,array $options = [],WriteConcern $writeConcern = null) {
        return $this->updateOperation($filter, $newObj ,$upsert, $options, $writeConcern, false);
    }

    /**
     * 修改多个文档
     * @param $filter
     * @param $newObj
     * @param bool $upsert
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @return \MongoDB\Driver\WriteResult
     */
    public function updateMany($filter, $newObj ,bool $upsert = false,array $options = [],WriteConcern $writeConcern = null)
    {
        return $this->updateOperation($filter, $newObj ,$upsert, $options, $writeConcern, true);
    }

    private function updateOperation($filter, $newObj ,bool $upsert = false,array $options = [],WriteConcern $writeConcern = null, bool $multiple = true)
    {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $updateOptions = array(
            'multi' => $multiple,//修改所有匹配到的文档
            'upsert' => $upsert,//是否在没有匹配到修改文档时，插入数据。
        );
        $bulk->update($filter,$newObj ,$updateOptions);
        $collectionName = $this->_coll;
        $databaseName = $this->_db;
        $writeResult  = $this->_conn->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }

    /**
     * 查询单条记录
     * @param array $filter
     * @param array $projection
     * @return array
     */
    function findOne(array $filter = [], array $projection = []){

        $this->limit = 1;

        $this->currentCursor = $this->find($filter,$projection,false)->is_query();

        $document = $this->object_array(current($this->currentCursor->toArray()));

        return $document;
    }

    /**
     * 查询多个文档
     * @param $filter
     * @param array $projection
     * @param int $limit
     * @param int $skip
     * @param array $sort
     * @param ReadPreference|NULL $readPreference
     * @return \MongoDB\Driver\Cursor
     */
    public function findMany($filter=array(), array $projection = [],int $limit = 0,int $skip = 0,array $sort = [], ReadPreference $readPreference = null){
        $newProject = array();
        foreach($projection as $project){
            $newProject[$project] = 1;
        }
        $queryOptions = [
            'projection' => $newProject,
            'limit' => $limit,
            'skip' => $skip,
            'sort' => $sort
        ];
        $cursor = $this->query($filter,$queryOptions,$readPreference);
        return $cursor;
    }

    /**
     * @param $filter
     * @param array $options
     * @param WriteConcern|NULL $writeConcern
     * @param bool $limit
     * @return \MongoDB\Driver\WriteResult
     * 删除方法
     */
    public function delete($filter,array $options = [],WriteConcern $writeConcern = null, bool $limit = false)
    {
        if(empty($writeConcern)){
            $writeConcern = new WriteConcern(WriteConcern::MAJORITY, 5000);
        }
        $bulk = new BulkWrite($options);
        $deleteOptions = array(
            'limit' => $limit,//是否删除只删除单数据。
        );
        $bulk->delete($filter,$deleteOptions);
        $collectionName = $this->_coll;
        $databaseName = $this->_db;
        $writeResult  = $this->_conn->executeBulkWrite($databaseName.'.'.$collectionName, $bulk, $writeConcern);
        return $writeResult;
    }
}

/**
 * Class mongoCursor
 * 游标类
 */
class mongoCursor {

    private $currentCursor = null;

    private $queryProcess = array();

    private $mongoCollection;

    private $is_iterator;

    private $skip = 0;
    private $limit = 0;
    private $sort = array();

    public function __construct($db, $is_iterator=true)
    {
        $this->mongoCollection = $db;
        $this->is_iterator = $is_iterator;
        $this->skip = $db->skip;
        $this->limit = $db->limit;
        $this->sort = $db->sort;
    }

    function next() {
        $this->currentCursor = $this->is_query();
        $result = $this->currentCursor->current();
        $this->currentCursor->next();
        return $this->mongoCollection->object_array($result);
    }

    function getNext() {
        return $this->next();
    }

    /**
     * 获取查询的游标对象
     * @param bool $is_iterator
     * @return mixed
     */
    public function is_query($is_new=false){
        $key_str =  md5(serialize($this->mongoCollection->filter) . $this->mongoCollection->_db . $this->mongoCollection->_coll);
        if($is_new || !isset($this->queryProcess[$key_str])){
            $cursor = $this->mongoCollection->findMany($this->mongoCollection->filter,$this->mongoCollection->projection,$this->limit,$this->skip,$this->sort);
            $this->queryProcess[$key_str] = $this->is_iterator ? new \IteratorIterator($cursor) : $cursor;
            $this->is_iterator ? $this->queryProcess[$key_str]->rewind() : '';
        }
        return $this->queryProcess[$key_str];
    }

    public function count(){
        return $this->mongoCollection->count($this->mongoCollection->filter);
    }

    public function sort($sort){
        $this->sort = $sort;
        return $this;
    }

    public function skip($skip){
        $this->skip = $skip;

        return $this;
    }

    public function limit($limit){
        $this->limit = $limit;
        return $this;
    }
}


