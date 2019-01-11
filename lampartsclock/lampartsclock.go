package lampartsclock

import (
    "sync"
)

type LampartsClock struct {
    Time int64
    Lock sync.RWMutex
}

func (c *LampartsClock) Get() int64 {
    c.Lock.RLock()
    defer c.Lock.RUnlock()
    t := c.Time
    return t
}

func (c *LampartsClock) GetAndInc() int64 {
    c.Lock.RLock()
    defer c.Lock.RUnlock()
    c.Time++
    t := c.Time
    return t
}

func (c *LampartsClock) SetIfHigherAndInc(t int64) {
    c.Lock.Lock()
    defer c.Lock.Unlock()
    if c.Time < t {
        c.Time = t
    }
    c.Time++
}

func (c *LampartsClock) Inc() {
    c.Lock.Lock()
    defer c.Lock.Unlock()
    c.Time++
}
