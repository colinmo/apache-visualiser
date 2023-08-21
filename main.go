package main

func main() {
	ConnectToDB()
	defer ReleaseDB()
	displayMainWindow()
}
