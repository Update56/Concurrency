package main

import (
	"concurrency/miner"
	"concurrency/postman"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func main() {

	// Хранилище угля
	var iron atomic.Int64

	// Хранилище писем
	var mails []string
	mtx := sync.Mutex{}

	// Создаём отдельные контексты для шахтёров и почтальонов
	// Для возможности отдельно завершать шахтёров и отдельно завершать почтальонов
	minerContex, minerCancel := context.WithCancel(context.Background())
	postmanContex, postmanCancel := context.WithCancel(context.Background())

	// Через 3 секунды мы завершим рабочий день шахтёров
	go func() {
		time.Sleep(3 * time.Second)
		fmt.Println("-----Рабочий день шахтеров окончен-----")
		minerCancel()
	}()

	// Через 6 секунд мы завершим рабочий день почтальонов
	go func() {
		time.Sleep(6 * time.Second)
		fmt.Println("-----Рабочий день почтальонов окончен-----")
		postmanCancel()
	}()

	// Запускаем 10х шахтёров, получаем пункт передачи железа
	ironTransferPoint := miner.MinerPool(minerContex, 10)

	// Запускаем 10х почтальонов, получаем пункт передачи писем
	mailTransferPoint := postman.PostmanPool(postmanContex, 10)

	wg := &sync.WaitGroup{}

	// В отдельной горутине вычитываем входящее железо
	wg.Add(1)
	go func() {
		defer wg.Done()

		for v := range ironTransferPoint {
			iron.Add(int64(v))

		}
	}()

	// В отдельной горутине вычитываем входящие письма
	wg.Add(1)
	go func() {
		defer wg.Done()

		for v := range mailTransferPoint {
			mtx.Lock()
			mails = append(mails, v)
			mtx.Unlock()
		}
	}()

	wg.Wait()

	// Выводим результирующие значения

	fmt.Println("Суммарно добытое железо:", iron.Load())

	mtx.Lock()
	fmt.Println("Суммарное кол-во полученных писем:", len(mails))
	mtx.Unlock()

}
