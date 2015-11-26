<?php

class mysql_pool
{
    public static $connections = array();
    protected static $config = null;
    public static function init($config)
    {
        self::$config = $config;
    }
    public static function getConnection($param)
    {
        $host = $param['host'];
        $user = $param['user'];
        $password    = $param['pass'];
        $db_name     = null;
        $port = $param['port'];

        if (isset($param['port'])) $conn = new mysqli($host, $user, $password, "", $param['port']);
        else {
            $conn = new mysqli($host, $user, $password);
        }
        $conn->query("set names utf8");
        return $conn;
    }

    public static function getConnByThread($thread_id)
    {
        if (isset(self::$connections[$thread_id])) {
            $conn = self::$connections[$thread_id];
            if ($conn->ping()) {
                self::$connections[$thread_id] = $conn;
                return $conn;
            }
        }
        $conn = mysql_pool::getConnection(
            array(
                'host' => self::$config->host,
                'user' => self::$config->user,
                'pass' => self::$config->pass,
                'port' => self::$config->port
            )
        );
        $conn->select_db(self::$config->db);
        self::$connections[$thread_id] = $conn;
        return self::$connections[$thread_id];
    }

    public function close($thread_id)
    {
        if (isset(self::$connections[$thread_id])) {
            self::$connections[$thread_id]->close();
            unset(self::$connections[$thread_id]);
        }
    }
}

function getSource($config)
{
    if (empty($config->source)) {
        return fopen('php://stdin', 'r');
    }
    if (!file_exists($config->source)) {
        echo "\nIncorrect file path {$config->source}";
        return;
    } else {
        return fopen($config->source, 'r');
    }
}

class parser
{
    public static function init($config)
    {
        $class = "{$config->parser_type}_parser";

        if (class_exists($class)) {
            return new $class();
        }
    }
}

class generallog_parser extends parser
{
    public $queries = array();
    protected $composite_query = array();
    protected $last_thread_id = null;

    public function readLine($row)
    {
        $row = trim($row);
        $aM = array();

        if (empty($row)) {
            return;
        }
        if (preg_match("~SET\s*GLOBAL\s*general_log\s*=~", $row)
            || preg_match("~\d++\s+Statistics~", $row)) {
            echo "\nignoring: $row";
            return;
        }
                 
        $aM = array();
        if (preg_match("~(\d++)\s+(Query|Init DB)\s++([^$]+)~", $row, $aM)) {
            if (!empty($this->composite_query[$aM[1]])) {
                $this->queries[$aM[1]][] = implode(" ", $this->composite_query[$aM[1]]);
                $this->composite_query[$aM[1]] = array();
            }
            $tmp_q = $this->cleanQuery($aM[3]);            
            if (preg_match("~Init db~i", $aM[2])) {
                $tmp_q = $aM[2] . ": " . $aM[3];
            }
            $this->composite_query[$aM[1]][] = $tmp_q;
            $this->last_thread_id = $aM[1];
        } elseif (preg_match("~(\d++)\s+Quit[^$]*~", $row, $aM)) {
            if (isset($this->composite_query[$aM[1]]) and !empty($this->composite_query[$aM[1]])) {
                $this->queries[$aM[1]][] = implode(" ", $this->composite_query[$aM[1]]);
                $this->queries[$aM[1]][] = 'Quit';
                unset($this->composite_query[$aM[1]]);
            }
            $this->last_thread_id = null;
        } elseif (preg_match("~(\d++)\s+Connect\s++([^$]+)~", $row, $aM)) {
            return;
        } else {            
            if (isset($this->last_thread_id) && !is_null($this->last_thread_id) && $this->last_thread_id !== '') 
            {
                $this->composite_query[$this->last_thread_id][] = $row;
            }
        }
    }
    /**
     * For removing strings like
     * 998774 Query        SELECT id,name FROM api_content_provider 151120  6:17:02 382578 Quit
     * @param string $query
     * @return correct mysql query
     */
    protected function cleanQuery($query)
    {
        return preg_replace(
            array('~\s+\d++\s+\d{1,2}:\d{2}:\d{2}\s+\d++\s+Quit~'),
            array(''),
            $query
        );
    }

    public function getQueries()
    {
        $queries2send = array();
        if (isset($this->queries) && !empty($this->queries)) {
            foreach ($this->queries as $thread_id => $queries) {
                foreach ($queries as $query) {
                    $queries2send[$thread_id][] = new query($query);
                }
            }
        }
        $this->queries = null;
        return $queries2send;
    }
}

class query 
{
    public $sql = '';
    public $handle_method = 'query';

    public function __construct($query)
    {
        if (empty($query)) {
            return;
        }
        if (preg_match("~^Init db~i", $query)) {
            $db = preg_replace("~^[^:]+:~", '', $query);
            $db = trim($db);
            $this->sql = $db;
            $this->handle_method = 'select_db';
            return;
        }
        
        if ($query == 'Quit') {            
            $this->sql = $query;
            $this->handle_method = 'thread_close';
            return;
        }
        $query = trim($query);
        $this->sql = $query;
        $this->handle_method = 'async_query';
    }

    public function async_query($conn, $query, $thread_id)
    {
        $res = $conn->query($query, MYSQLI_ASYNC);
        if (!$res) {
            echo "\nerror: {$conn->error}";
        } else if (is_object($res)) {
            $res->free();
        }
    }
    
    public function thread_close($conn, $query, $thread_id)
    {
        echo "\nclose connection for $thread_id";
        $conn->close();
    }    
}