package canal

type CommandType string

const (
	RDB CommandType = "rdb"

	Ping CommandType = "ping"
	Pong CommandType = "pong"
	Ok   CommandType = "ok"
	Err  CommandType = "err"

	Empty     CommandType = "empty"
	Undefined CommandType = "undefined"

	Select    CommandType = "select"
	Zadd      CommandType = "zadd"
	Sadd      CommandType = "sadd"
	Zrem      CommandType = "zrem"
	Delete    CommandType = "delete"
	Lpush     CommandType = "lpush"
	LpushX    CommandType = "lpushx"
	Rpush     CommandType = "rpush"
	RpushX    CommandType = "rpushx"
	Rpop      CommandType = "rpop"
	RpopLpush CommandType = "rpoplpush"
	Lpop      CommandType = "lpop"

	ZremRangeByLex   CommandType = "zremrangebylex"
	ZremRangeByRank  CommandType = "zremrangebyrank"
	ZremRangeByScore CommandType = "zremrangebyscore"
	ZunionStore      CommandType = "zunionstore"
	ZincrBy          CommandType = "zincrby"
	SdiffStore       CommandType = "sdiffstore"
	SinterStore      CommandType = "sinterstore"
	Smove            CommandType = "smove"
	SunionStore      CommandType = "sunionstore"
	Srem             CommandType = "srem"
	Set              CommandType = "set"
	SetBit           CommandType = "setbit"

	Append       CommandType = "append"
	BitField     CommandType = "bitfield"
	BitOp        CommandType = "bitop"
	Decr         CommandType = "decr"
	DecrBy       CommandType = "decrby"
	Incr         CommandType = "incr"
	IncrBy       CommandType = "incrby"
	IncrByFloat  CommandType = "incrbyfloat"
	GetSet       CommandType = "getset"
	Mset         CommandType = "mset"
	MsetNX       CommandType = "msetnx"
	SetEX        CommandType = "setex"
	SetNX        CommandType = "setnx"
	SetRange     CommandType = "setrange"
	BlPop        CommandType = "blpop"
	BrPop        CommandType = "brpop"
	BrPopLpush   CommandType = "brpoplpush"
	Linsert      CommandType = "linsert"
	Lrem         CommandType = "lrem"
	Lset         CommandType = "lset"
	Ltrim        CommandType = "ltrim"
	Expire       CommandType = "expire"
	ExpireAt     CommandType = "expireat"
	Pexpire      CommandType = "pexpire"
	PexpireAt    CommandType = "pexpireat"
	Move         CommandType = "move"
	Persist      CommandType = "persist"
	Rename       CommandType = "rename"
	Restore      CommandType = "restore"
	Hset         CommandType = "hset"
	HsetNx       CommandType = "hsetnx"
	HmSet        CommandType = "hmset"
	HincrBy      CommandType = "hincrby"
	HincrByFloat CommandType = "hincrbyfloat"
	PfAdd        CommandType = "pfadd"
	PfMerge      CommandType = "pfmerge"
	PsetX        CommandType = "psetx"
)

var CommandTypeMap = map[string]CommandType{
	"select":           Select,
	"zadd":             Zadd,
	"sadd":             Sadd,
	"zrem":             Zrem,
	"delete":           Delete,
	"lpush":            Lpush,
	"lpushx":           LpushX,
	"rpush":            Rpush,
	"rpushx":           RpushX,
	"rpop":             Rpop,
	"rpoplpush":        RpopLpush,
	"lpop":             Lpop,
	"zremrangebylex":   ZremRangeByLex,
	"zremrangebyrank":  ZremRangeByRank,
	"zremrangebyscore": ZremRangeByScore,
	"zunionstore":      ZunionStore,
	"zincrby":          ZincrBy,
	"sdiffstore":       SdiffStore,
	"sinterstore":      SinterStore,
	"smove":            Smove,
	"sunionstore":      SunionStore,
	"srem":             Srem,
	"set":              Set,
	"setbit":           SetBit,
	"append":           Append,
	"bitfield":         BitField,
	"bitop":            BitOp,
	"decr":             Decr,
	"decrby":           DecrBy,
	"incr":             Incr,
	"incrby":           IncrBy,
	"incrbyfloat":      IncrByFloat,
	"getset":           GetSet,
	"mset":             Mset,
	"msetnx":           MsetNX,
	"setex":            SetEX,
	"setnx":            SetNX,
	"setrange":         SetRange,
	"blpop":            BlPop,
	"brpop":            BrPop,
	"brpoplpush":       BrPopLpush,
	"linsert":          Linsert,
	"lrem":             Lrem,
	"lset":             Lset,
	"ltrim":            Ltrim,
	"expire":           Expire,
	"expireat":         ExpireAt,
	"pexpire":          Pexpire,
	"pexpireat":        PexpireAt,
	"move":             Move,
	"persist":          Persist,
	"rename":           Rename,
	"restore":          Restore,
	"hset":             Hset,
	"hsetnx":           HsetNx,
	"hmset":            HmSet,
	"hincrby":          HincrBy,
	"hincrbyfloat":     HincrByFloat,
	"pfadd":            PfAdd,
	"pfmerge":          PfMerge,
	"psetx":            PsetX,
}
