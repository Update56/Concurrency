package miner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Функция, описывающая работу одного отдельного шахтёра
func miner(
	ctx context.Context,
	wg *sync.WaitGroup,
	transferPoint chan<- int,
	n int,
	power int,
) {
	defer wg.Done()

	for {
		// Этот шахтёр завершит своё выполнение, только после того
		// Как увидит сигнал о завершении рабочего дня через ctx
		// И доработает свою последнюю рабочую итерацию
		select {
		case <-ctx.Done():
			fmt.Println("Я шахтер ", n, " Мой рабочий день закончен")
			return
		default:
			fmt.Println("Я шахтер номер: ", n, " Начал добывать железо!")
			time.Sleep(1 * time.Second)
			fmt.Println("Я шахтер номер: ", n, " Добыл железо!")

			transferPoint <- power

			fmt.Println("Я шахтер номер: ", n, " Передал железо: ", power)
		}

	}
}

// func miner(
// 	ctx context.Context,
// 	wg *sync.WaitGroup,
// 	transferPoint chan<- int,
// 	n int,
// 	power int,
// ) {
// 	defer wg.Done()

// 	for {
// Этот шахтёр завершит своё выполнение СРАЗУ после сигнала о завершении рабочего дня
// 		fmt.Println("Я шахтер номер: ", n, " Начал добывать железо!")
// 		select {
// 		case <-ctx.Done():
// 			fmt.Println("Я шахтер ", n, " Мой рабочий день закончен")
// 			return
// 		case <-time.After(1 * time.Second):
// 			fmt.Println("Я шахтер номер: ", n, " Добыл железо!")

// 		}

// 		select {
// 		case <-ctx.Done():
// 			fmt.Println("Я шахтер ", n, " Мой рабочий день закончен")
// 			return
// 		case transferPoint <- power:
// 			fmt.Println("Я шахтер номер: ", n, " Передал железо: ", power)
// 		}
// 	}
// }

// MinerPool - функция, запускающая minerCount шахтёров,
// Позволяющая потребителям MinerPool функции получать железо от запущенных шахтёров
// И контролирующая закрытие канала общения
func MinerPool(ctx context.Context, minerCount int) <-chan int {
	ironTransferPoint := make(chan int)

	wg := &sync.WaitGroup{}

	for i := 1; i <= minerCount; i++ {
		wg.Add(1)
		go miner(ctx, wg, ironTransferPoint, i, i*10)
	}

	go func() {
		wg.Wait()
		close(ironTransferPoint)
	}()

	return ironTransferPoint
}
