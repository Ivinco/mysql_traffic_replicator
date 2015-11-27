<?php
//./tool --gnerallog --source=/path/to/general/log --host=mysql-test-host --user=username --password=pwd --port=3306
$config = new stdClass();
$config->host = 'localhost';
$config->user = 'root';
$config->pass = '';
$config->port = 3306;
$config->db   = 'test';
$config->source = null;
$config->parser_type = null;

$PATH = dirname(__FILE__) . "/";
include $PATH . 'lib.mysql.replicator.php';

if (!empty($argv[1])) {
    while ($sA = array_shift($argv)) {
        if (preg_match("%^-{0,2}help$%", $sA, $aM)) $bIsHelp = true;

        if (preg_match("%^-{1,2}host=(.+)$%", $sA, $aM)) {
            $config->host = trim($aM[1]);
        }
        if (preg_match("%^-{1,2}user=(.+)$%", $sA, $aM)) {
            $config->user = trim($aM[1]);
        }
        if (preg_match("%^-{1,2}password=(.+)$%", $sA, $aM)) {
            $config->pass = trim($aM[1]);
        }
        if (preg_match("%^--generallog$%", $sA)) {
            $config->parser_type = 'generallog';
        }
        if (preg_match("%^-{1,2}source=(.+)$%", $sA, $aM)) {
            $config->source = trim($aM[1]);
        }
    }
} else {
    showHelp();
}

function showHelp()
{
    exit ("\nusage:\nphp misc_scripts/" . __FILE__ . "  --generallog --source=/path/to/general/log --host=mysql-test-host --user=username --password=pwd --port=3306 \n");
}
mysql_pool::init($config);
$conn = mysql_pool::getConnection(
    array(
        'host' => $config->host,
        'user' => $config->user,
        'pass' => $config->pass,
        'port' => $config->port
    )
);
if (!is_object($conn) && !$conn->ping()) {
    echo "\nincorrect connection param";
    showHelp();
}
$conn->select_db($config->db);
$conn->close();
$parser = parser::init($config);

$fp = getSource($config);
do {
    while ($row = fgets($fp, 4096)) {
        $parser->readLine($row);
        $thread_queries = $parser->getQueries();

        if (!empty($thread_queries)) {
            foreach ($thread_queries as $thread_id => $queries) {
                $conn = mysql_pool::getConnByThread($thread_id);
    
                foreach ($queries as $query) {                
                    $method = $query->handle_method;
                    if (method_exists($conn, $query->handle_method)) {
                        $conn->$method($query->sql);
                    } else if (method_exists($query, $query->handle_method)) {
                        $query->$method($conn, $query->sql, $thread_id);
                    }
                }
            }
        }
    }
} while(true);
