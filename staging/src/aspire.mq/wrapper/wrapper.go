package wrapper

func Wrapper(f func(), funcName string) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				// 需要打印日志把funcName打印到里面
			}
			f()
		}()
	}()
}
