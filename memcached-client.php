<?php
/**
 * Created by PhpStorm.
 * User: gdcm2016
 * Date: 2017/12/11
 * Time: 16:49
 */

//
// +---------------------------------------------------------------------------+
// | memcached client, PHP                                                     |
// +---------------------------------------------------------------------------+
// | Copyright (c) 2003 Ryan T. Dean <rtdean@cytherianage.net>                 |
// | All rights reserved.                                                      |
// |                                                                           |
// | Redistribution and use in source and binary forms, with or without        |
// | modification, are permitted provided that the following conditions        |
// | are met:                                                                  |
// |                                                                           |
// | 1. Redistributions of source code must retain the above copyright         |
// |    notice, this list of conditions and the following disclaimer.          |
// | 2. Redistributions in binary form must reproduce the above copyright      |
// |    notice, this list of conditions and the following disclaimer in the    |
// |    documentation and/or other materials provided with the distribution.   |
// |                                                                           |
// | THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR      |
// | IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES |
// | OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.   |
// | IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,          |
// | INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT  |
// | NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, |
// | DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY     |
// | THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       |
// | (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF  |
// | THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.         |
// +---------------------------------------------------------------------------+
// | Author: Ryan T. Dean <rtdean@cytherianage.net>                            |
// | Heavily influenced by the Perl memcached client by Brad Fitzpatrick.      |
// |   Permission granted by Brad Fitzpatrick for relicense of ported Perl     |
// |   client logic under 2-clause BSD license.                                |
// +---------------------------------------------------------------------------+
//
// $TCAnet$
//


// {{{ requirements
// }}}

// {{{ constants
// {{{ flags

define("MEMCACHE_SERIALIZED", 1<<0);

define("MEMCACHE_COMPRESSED", 1<<1);

// }}}

define("COMPRESSION_SAVINGS", 0.20);

// }}}

// {{{ class memcached
class memcached
{
    // {{{ properties
    // {{{ public


    var $stats;

    // }}}
    // {{{ private


    var $_cache_sock;


    var $_debug;


    var $_host_dead;


    var $_have_zlib;


    var $_compress_enable;


    var $_compress_threshold;


    var $_persistant;


    var $_single_sock;


    var $_servers;


    var $_buckets;


    var $_bucketcount;


    var $_active;

    // }}}
    // }}}
    // {{{ methods
    // {{{ public functions
    // {{{ memcached()


    function memcached ($args)
    {
        $this->set_servers($args['servers']);
        $this->_debug = $args['debug'];
        $this->stats = array();
        $this->_compress_threshold = $args['compress_threshold'];
        $this->_persistant = isset($args['persistant']) ? $args['persistant'] : false;
        $this->_compress_enable = true;
        $this->_have_zlib = function_exists("gzcompress");

        $this->_cache_sock = array();
        $this->_host_dead = array();
    }

    // }}}
    // {{{ add()


    function add ($key, $val, $exp = 0)
    {
        return $this->_set('add', $key, $val, $exp);
    }

    // }}}
    // {{{ decr()


    function decr ($key, $amt=1)
    {
        return $this->_incrdecr('decr', $key, $amt);
    }

    // }}}
    // {{{ delete()


    function delete ($key, $time = 0)
    {
        if (!$this->_active)
            return false;

        $sock = $this->get_sock($key);
        if (!is_resource($sock))
            return false;

        $key = is_array($key) ? $key[1] : $key;

        $this->stats['delete']++;
        $cmd = "delete $key $time\r\n";
        if(!fwrite($sock, $cmd, strlen($cmd)))
        {
            $this->_dead_sock($sock);
            return false;
        }
        $res = trim(fgets($sock));

        if ($this->_debug)
            printf("MemCache: delete %s (%s)\n", $key, $res);

        if ($res == "DELETED")
            return true;
        return false;
    }

    // }}}
    // {{{ disconnect_all()


    function disconnect_all ()
    {
        foreach ($this->_cache_sock as $sock)
            fclose($sock);

        $this->_cache_sock = array();
    }

    // }}}
    // {{{ enable_compress()


    function enable_compress ($enable)
    {
        $this->_compress_enable = $enable;
    }

    // }}}
    // {{{ forget_dead_hosts()


    function forget_dead_hosts ()
    {
        $this->_host_dead = array();
    }

    // }}}
    // {{{ get()


    function get ($key)
    {
        if (!$this->_active)
            return false;

        $sock = $this->get_sock($key);

        if (!is_resource($sock))
            return false;

        $this->stats['get']++;

        $cmd = "get $key\r\n";
        if (!fwrite($sock, $cmd, strlen($cmd)))
        {
            $this->_dead_sock($sock);
            return false;
        }

        $val = array();
        $this->_load_items($sock, $val);

        if ($this->_debug)
            foreach ($val as $k => $v)
                printf("MemCache: sock %s got %s => %s\r\n", $sock, $k, $v);

        return $val[$key];
    }

    // }}}
    // {{{ get_multi()


    function get_multi ($keys)
    {
        if (!$this->_active)
            return false;

        $this->stats['get_multi']++;

        foreach ($keys as $key)
        {
            $sock = $this->get_sock($key);
            if (!is_resource($sock)) continue;
            $key = is_array($key) ? $key[1] : $key;
            if (!isset($sock_keys[$sock]))
            {
                $sock_keys[$sock] = array();
                $socks[] = $sock;
            }
            $sock_keys[$sock][] = $key;
        }

        // Send out the requests
        foreach ($socks as $sock)
        {
            $cmd = "get";
            foreach ($sock_keys[$sock] as $key)
            {
                $cmd .= " ". $key;
            }
            $cmd .= "\r\n";

            if (fwrite($sock, $cmd, strlen($cmd)))
            {
                $gather[] = $sock;
            } else
            {
                $this->_dead_sock($sock);
            }
        }

        // Parse responses
        $val = array();
        foreach ($gather as $sock)
        {
            $this->_load_items($sock, $val);
        }

        if ($this->_debug)
            foreach ($val as $k => $v)
                printf("MemCache: got %s => %s\r\n", $k, $v);

        return $val;
    }

    // }}}
    // {{{ incr()


    function incr ($key, $amt=1)
    {
        return $this->_incrdecr('incr', $key, $amt);
    }

    // }}}
    // {{{ replace()


    function replace ($key, $value, $exp=0)
    {
        return $this->_set('replace', $key, $value, $exp);
    }

    // }}}
    // {{{ run_command()


    function run_command ($sock, $cmd)
    {
        if (!is_resource($sock))
            return array();

        if (!fwrite($sock, $cmd, strlen($cmd)))
            return array();

        while (true)
        {
            $res = fgets($sock);
            $ret[] = $res;
            if (preg_match('/^END/', $res))
                break;
            if (strlen($res) == 0)
                break;
        }
        return $ret;
    }

    // }}}
    // {{{ set()


    function set ($key, $value, $exp=0)
    {
        return $this->_set('set', $key, $value, $exp);
    }

    // }}}
    // {{{ set_compress_threshold()


    function set_compress_threshold ($thresh)
    {
        $this->_compress_threshold = $thresh;
    }

    // }}}
    // {{{ set_debug()


    function set_debug ($dbg)
    {
        $this->_debug = $dbg;
    }

    // }}}
    // {{{ set_servers()


    function set_servers ($list)
    {
        $this->_servers = $list;
        $this->_active = count($list);
        $this->_buckets = null;
        $this->_bucketcount = 0;

        $this->_single_sock = null;
        if ($this->_active == 1)
            $this->_single_sock = $this->_servers[0];
    }

    // }}}
    // }}}
    // {{{ private methods
    // {{{ _close_sock()


    function _close_sock ($sock)
    {
        $host = array_search($sock, $this->_cache_sock);
        fclose($this->_cache_sock[$host]);
        unset($this->_cache_sock[$host]);
    }

    // }}}
    // {{{ _connect_sock()


    function _connect_sock (&$sock, $host, $timeout = 0.25)
    {
        list ($ip, $port) = explode(":", $host);
        if ($this->_persistant == 1)
        {
            $sock = @pfsockopen($ip, $port, $errno, $errstr, $timeout);
        } else
        {
            $sock = @fsockopen($ip, $port, $errno, $errstr, $timeout);
        }

        if (!$sock)
            return false;
        return true;
    }

    // }}}
    // {{{ _dead_sock()


    function _dead_sock ($sock)
    {
        $host = array_search($sock, $this->_cache_sock);
        list ($ip, $port) = explode(":", $host);
        $this->_host_dead[$ip] = time() + 30 + intval(rand(0, 10));
        $this->_host_dead[$host] = $this->_host_dead[$ip];
        unset($this->_cache_sock[$host]);
    }

    // }}}
    // {{{ get_sock()


    function get_sock ($key)
    {
        if (!$this->_active)
            return false;

        if ($this->_single_sock !== null)
            return $this->sock_to_host($this->_single_sock);

        $hv = is_array($key) ? intval($key[0]) : $this->_hashfunc($key);

        if ($this->_buckets === null)
        {
            foreach ($this->_servers as $v)
            {
                if (is_array($v))
                {
                    for ($i=0; $i<$v[1]; $i++)
                        $bu[] = $v[0];
                } else
                {
                    $bu[] = $v;
                }
            }
            $this->_buckets = $bu;
            $this->_bucketcount = count($bu);
        }

        $realkey = is_array($key) ? $key[1] : $key;
        for ($tries = 0; $tries<20; $tries++)
        {
            $host = $this->_buckets[$hv % $this->_bucketcount];
            $sock = $this->sock_to_host($host);
            if (is_resource($sock))
                return $sock;
            $hv += $this->_hashfunc($tries . $realkey);
        }

        return false;
    }

    // }}}
    // {{{ _hashfunc()


    function _hashfunc ($key)
    {
        $hash = 0;
        for ($i=0; $i<strlen($key); $i++)
        {
            $hash = $hash*33 + ord($key[$i]);
        }

        return $hash;
    }

    // }}}
    // {{{ _incrdecr()


    function _incrdecr ($cmd, $key, $amt=1)
    {
        if (!$this->_active)
            return null;

        $sock = $this->get_sock($key);
        if (!is_resource($sock))
            return null;

        $key = is_array($key) ? $key[1] : $key;
        $this->stats[$cmd]++;
        if (!fwrite($sock, "$cmd $key $amt\r\n"))
            return $this->_dead_sock($sock);

        stream_set_timeout($sock, 1, 0);
        $line = fgets($sock);
        if (!preg_match('/^(\d+)/', $line, $match))
            return null;
        return $match[1];
    }

    // }}}
    // {{{ _load_items()


    function _load_items ($sock, &$ret)
    {
        while (1)
        {
            $decl = fgets($sock);
            if ($decl == "END\r\n")
            {
                return true;
            } elseif (preg_match('/^VALUE (\S+) (\d+) (\d+)\r\n$/', $decl, $match))
            {
                list($rkey, $flags, $len) = array($match[1], $match[2], $match[3]);
                $bneed = $len+2;
                $offset = 0;

                while ($bneed > 0)
                {
                    $data = fread($sock, $bneed);
                    $n = strlen($data);
                    if ($n == 0)
                        break;
                    $offset += $n;
                    $bneed -= $n;
                    $ret[$rkey] .= $data;
                }

                if ($offset != $len+2)
                {
                    // Something is borked!
                    if ($this->_debug)
                        printf("Something is borked!  key %s expecting %d got %d length\n", $rkey, $len+2, $offset);

                    unset($ret[$rkey]);
                    $this->_close_sock($sock);
                    return false;
                }

                $ret[$rkey] = rtrim($ret[$rkey]);

                if ($this->_have_zlib && $flags & MEMCACHE_COMPRESSED)
                    $ret[$rkey] = gzuncompress($ret[$rkey]);

                if ($flags & MEMCACHE_SERIALIZED)
                    $ret[$rkey] = unserialize($ret[$rkey]);

            } else
            {
                if ($this->_debug)
                    print("Error parsing memcached response\n");
                return 0;
            }
        }
    }

    // }}}
    // {{{ _set()


    function _set ($cmd, $key, $val, $exp)
    {
        if (!$this->_active)
            return false;

        $sock = $this->get_sock($key);
        if (!is_resource($sock))
            return false;

        $this->stats[$cmd]++;

        $flags = 0;

        if (!is_scalar($val))
        {
            $val = serialize($val);
            $flags |= MEMCACHE_SERIALIZED;
            if ($this->_debug)
                printf("client: serializing data as it is not scalar\n");
        }

        $len = strlen($val);

        if ($this->_have_zlib && $this->_compress_enable &&
            $this->_compress_threshold && $len >= $this->_compress_threshold)
        {
            $c_val = gzcompress($val, 9);
            $c_len = strlen($c_val);

            if ($c_len < $len*(1 - COMPRESS_SAVINGS))
            {
                if ($this->_debug)
                    printf("client: compressing data; was %d bytes is now %d bytes\n", $len, $c_len);
                $val = $c_val;
                $len = $c_len;
                $flags |= MEMCACHE_COMPRESSED;
            }
        }
        if (!fwrite($sock, "$cmd $key $flags $exp $len\r\n$val\r\n"))
            return $this->_dead_sock($sock);

        $line = trim(fgets($sock));

        if ($this->_debug)
        {
            if ($flags & MEMCACHE_COMPRESSED)
                $val = 'compressed data';
            printf("MemCache: %s %s => %s (%s)\n", $cmd, $key, $val, $line);
        }
        if ($line == "STORED")
            return true;
        return false;
    }

    // }}}
    // {{{ sock_to_host()


    function sock_to_host ($host)
    {
        if (isset($this->_cache_sock[$host]))
            return $this->_cache_sock[$host];

        $now = time();
        list ($ip, $port) = explode (":", $host);
        if (isset($this->_host_dead[$host]) && $this->_host_dead[$host] > $now ||
            isset($this->_host_dead[$ip]) && $this->_host_dead[$ip] > $now)
            return null;

        if (!$this->_connect_sock($sock, $host))
            return $this->_dead_sock($host);

        // Do not buffer writes
        stream_set_write_buffer($sock, 0);

        $this->_cache_sock[$host] = $sock;

        return $this->_cache_sock[$host];
    }

    // }}}
    // }}}
    // }}}
}

// }}}
?>