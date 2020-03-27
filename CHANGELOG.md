## 2019.03.02

* fix panic on access to index after pool close

## 2019.03.1

* added session pre-creation limit check in pool
* added discovery trigger on more then half unhealthy transport connects
* await transport connect only if no healthy connections left

## 2019.02

* support cloud IAM (jwt) authorization from service account file
* minimum version of Go become 1.13. Started support of new `errors` features
