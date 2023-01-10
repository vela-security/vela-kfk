package kfk

import (
	"github.com/vela-security/vela-public/assert"
	"github.com/vela-security/vela-public/lua"
)

var xEnv assert.Environment

/*
	local pro = vela.kfk.producer{
		name = "123",
	}

	pro.limit(100)
	pro.start()

	pro.push("topic" , [[{"name":"123" , "passwd":"123456"}]])
	pro.push("topic" , [[{"name":"123" , "passwd":"123456"}]])
	pro.push("topic" , [[{"name":"123" , "passwd":"123456"}]])
	pro.push("topic" , [[{"name":"123" , "passwd":"123456"}]])
	pro.push("topic" , [[{"name":"123" , "passwd":"123456"}]])
	pro.push("topic" , [[{"name":"123" , "passwd":"123456"}]])

	local switch = vela.switch()
	switch._{}
	switch.default(function(msg)
	end)

	local c = vela.kfk.consumer{}

    cli.read("topic" , "topic2" , "topic3")
	cli.codec(kfk.map , kfk)
	cli.pipe(function(raw)
	end)

    kfk.select(v)

*/

func newLuaProducer(L *lua.LState) int {
	cfg := newConfig(L)
	proc := L.NewProc(cfg.name, producerTypeOf)
	if proc.IsNil() {
		proc.Set(NewProducer(cfg))
	} else {
		proc.Data.(*Producer).cfg = cfg
	}

	L.Push(proc)
	return 1
}

func newLuaConsumer(L *lua.LState) int {
	cfg := newConfig(L)
	proc := L.NewProc(cfg.name, consumerTypeOf)
	if proc.IsNil() {
		proc.Set(NewConsumer(cfg))
	} else {
		proc.Data.(*Consumer).cfg = cfg
	}

	L.Push(proc)
	return 1
}

func WithEnv(env assert.Environment) {
	xEnv = env
	kfk := lua.NewUserKV()
	kfk.Set("producer", lua.NewFunction(newLuaProducer))
	kfk.Set("consumer", lua.NewFunction(newLuaConsumer))
	xEnv.Set("kfk", kfk)
}
