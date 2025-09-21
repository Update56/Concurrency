package postman

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Функция, описывающая работу одного отдельного почтальона
func postman(
	ctx context.Context,
	wg *sync.WaitGroup,
	transferPoint chan<- string,
	n int,
	mail string,
) {
	defer wg.Done()

	for {
		// Этот почтально завершит своё выполнение, только после того
		// Как увидит сигнал о завершении рабочего дня через ctx
		// И доработает свою последнюю рабочую итерацию
		select {
		case <-ctx.Done():
			fmt.Println("Я почтальон: ", n, " Мой рабочий день закончен")
			return
		default:
			fmt.Println("Я почтальон номер: ", n, " Взял письмо!")
			time.Sleep(1 * time.Second)
			fmt.Println("Я почтальон номер: ", n, " Донес письмо:", mail)

			transferPoint <- mail

			fmt.Println("Я шахтер номер: ", n, " Передал письмо: ", mail)
		}

	}
}

// PostmanPool -- функция, запускающая postmanCount почтальонов,
// Позволяющая потребителям PostmanPool функции получать письма от запущенных почтальонов
// И контролирующая закрытие канала общения
func PostmanPool(ctx context.Context, postmanCount int) <-chan string {
	mailTransferPoint := make(chan string)

	wg := &sync.WaitGroup{}

	for i := 1; i <= postmanCount; i++ {
		wg.Add(1)
		go postman(ctx, wg, mailTransferPoint, i, PostmanToMail(i))
	}

	go func() {
		wg.Wait()
		close(mailTransferPoint)
	}()

	return mailTransferPoint
}

// Заготовка конкретных писем для конкретных почтальонов
// Если для какого-то почтальона нет заготовленного письма, то этот почтальон разносит лотерею
func PostmanToMail(postmanNumber int) string {
	ptm := map[int]string{
		1: "Всем привет!",
		2: "Уведомление",
		3: "Приглашение",
	}
	mail, ok := ptm[postmanNumber]
	if !ok {
		return "Лотерея"
	}
	return mail
}
