package common

import (
	"crypto/rand"
	"fmt"
	"image"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func GenerateRandomHex(size int) string {
	key := make([]byte, size)
	_, err := rand.Read(key)
	if err != nil {
		log.Error("helper.go:15 Failed rand.Read")
	}

	return fmt.Sprintf("%x", key)
}

func StrIsNumber(str string) bool {
	digitCheck := regexp.MustCompile(`^[0-9]+$`)
	return digitCheck.MatchString(str)
}

func CleanSpecialChar(str string) string {
	str = strings.TrimSpace(str)
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}
	cleanStr := reg.ReplaceAllString(str, "")
	return cleanStr
}

func StrToInt(str string) int {
	intVar, _ := strconv.Atoi(str)
	return intVar
}

func IntToStr(val int) string {
	return strconv.Itoa(val)
}

func IsValidDateFormat(val string) bool {
	re := regexp.MustCompile("^(19|20)\\d\\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])$")
	return re.MatchString(val)
}

func FormatNumber(num int) string {
	str := fmt.Sprintf("%d", num)
	re := regexp.MustCompile("(\\d+)(\\d{3})")
	for n := ""; n != str; {
		n = str
		str = re.ReplaceAllString(str, "$1.$2")
	}
	return str
}

func FindInArray(val interface{}, array interface{}) (exists bool, index int) {
	exists = false
	index = -1

	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				index = i
				exists = true
				return
			}
		}
	}

	return
}

func IntersectionInArrayString(a, b []string) (c []string) {
	m := make(map[string]bool)

	for _, item := range a {
		m[item] = true
	}

	for _, item := range b {
		if _, ok := m[item]; ok {
			c = append(c, item)
		}
	}
	return
}

func RemoveSpace(input string) string {
	re_leadclose_whtsp := regexp.MustCompile(`^[\s\p{Zs}]+|[\s\p{Zs}]+$`)
	re_inside_whtsp := regexp.MustCompile(`[\s\p{Zs}]{2,}`)
	final := re_leadclose_whtsp.ReplaceAllString(input, "")
	final = re_inside_whtsp.ReplaceAllString(final, " ")
	return final
}

func RemoveDoubleSpace(data string) string {
	space := regexp.MustCompile(`\s+`)
	return space.ReplaceAllString(data, " ")
}

func SortData(data interface{}) []int {
	var urutan []int
	for _, v := range strings.Split(fmt.Sprint(data), ",") {
		if a, err := strconv.Atoi(v); err == nil {
			urutan = append(urutan, a)
		}
	}

	sort.Ints(urutan)
	return urutan
}

func IsNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func IsValidUrl(toTest string) bool {
	_, err := url.ParseRequestURI(toTest)
	if err != nil {
		return false
	}

	u, err := url.Parse(toTest)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}

	return true
}

func IsValidEmail(email string) bool {
	Re := regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}$`)
	return Re.MatchString(email)
}

func GetImageDimension(imagePath string) (int, int, error) {
	file, err := os.Open(imagePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	_image, _, err := image.DecodeConfig(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", imagePath, err)
	}
	return _image.Width, _image.Height, err
}
