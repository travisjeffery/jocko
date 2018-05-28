package structs

type TopicConfig map[string]TopicConfigEntry

func NewTopicConfig() TopicConfig {
	cfg := make(TopicConfig)

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "cleanup.policy",
			Default: "delete",
		},
		ServerDefault: "log.cleanup.policy",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "compression.type",
			Default: "producer",
		},
		ServerDefault: "compression.type",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "delete.retention.ms",
			Default: 86400000,
		},
		ServerDefault: "log.cleaner.delete.retention.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "file.delete.delay.ms",
			Default: 60000,
		},
		ServerDefault: "log.segment.delete.delay.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "flush.messages",
			Default: 9223372036854,
		},
		ServerDefault: "log.flush.interval.messages",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "flush.ms",
			Default: 9223372036854775807,
		},
		ServerDefault: "log.flush.interval.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name: "follower.replication.throttled.replicas",
		},
		ServerDefault: "follower.replication.throttled.replicas",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "index.interval.bytes",
			Default: 4096,
		},
		ServerDefault: "log.index.interval.bytes",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name: "leader.replication.throttled.replicas",
		},
		ServerDefault: "leader.replication.throttled.replicas",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "max.message.bytes",
			Default: 1000012,
		},
		ServerDefault: "message.max.bytes",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "message.format.version",
			Default: "1.1-IV0",
		},
		ServerDefault: "log.message.format.version",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "message.timestamp.difference.max.ms",
			Default: 9223372036854775807,
		},
		ServerDefault: "log.message.timestamp.difference.max.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "message.timestamp.type",
			Default: "CreateTime",
		},
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "min.cleanable.dirty.ratio",
			Default: 0.5,
		},
		ServerDefault: "log.cleaner.min.cleanable.ratio",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "min.compaction.lag.ms",
			Default: 0,
		},
		ServerDefault: "log.cleaner.min.compaction.lag.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "min.insync.replicas",
			Default: 1,
		},
		ServerDefault: "min.insync.replicas",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "preallocate",
			Default: false,
		},
		ServerDefault: "log.preallocate",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "retention.bytes",
			Default: -1,
		},
		ServerDefault: "log.retention.bytes",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "retention.ms",
			Default: 604800000,
		},
		ServerDefault: "log.retention.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "segment.bytes",
			Default: 1073741824,
		},
		ServerDefault: "log.segment.bytes",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "segment.index.bytes",
			Default: 10485760,
		},
		ServerDefault: "log.index.size.max.bytes",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "segment.jitter.ms",
			Default: 0,
		},
		ServerDefault: "log.roll.jitter.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "segment.ms",
			Default: 604800000,
		},
		ServerDefault: "log.roll.ms",
	})

	cfg.Set(TopicConfigEntry{
		ConfigEntry: ConfigEntry{
			Name:    "unclean.leader.election.enable",
			Default: "false",
		},
		ServerDefault: "unclean.leader.election.enable",
	})

	return cfg
}

func (c TopicConfig) Set(e TopicConfigEntry) {
	c[e.Name] = e
}

func (c TopicConfig) Get(name string) TopicConfigEntry {
	return c[name]
}

func (c TopicConfig) GetValue(name string) interface{} {
	e, ok := c[name]
	if !ok {
		return nil
	}
	if e.Value != nil {
		return e.Value
	}
	return e.Default
}

func (c TopicConfig) SetValue(name string, value interface{}) TopicConfig {
	e, ok := c[name]
	if !ok {
		return c
	}
	e.Value = value
	c[name] = e
	return c
}

type ConfigEntry struct {
	Default     interface{}
	Name        string
	ValidValues []interface{}
	Value       interface{}
}

type TopicConfigEntry struct {
	ConfigEntry
	ServerDefault string
}
