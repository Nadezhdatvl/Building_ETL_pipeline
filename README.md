# Building_ETL_pipeline
Karpov courses

# Проект 1: Построение ETL-Пайплайна

### 1. Описане проекта:
В нашем проекте мы будем решать задачу на ETL схему и как это можно сделать при помощи библиотеки Airflow.
В целом Airflow — это удобный инструмент для решения ETL-задач.
Мы напишем task-и на python, которые Airflow будет исполнять. Ожидается, что на выходе будет DAG в Airflow, который будет считаться каждый день за вчера.

#### Наша задача:
* Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
* Далее объединяем две таблицы в одну.
* Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
* И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
* Каждый день таблица должна дополняться новыми данными.

#### Структура финальной таблицы должна быть такая:

* Дата - event_date
* Название среза - dimension
* Значение среза - dimension_value
* Число просмотров - views
* Числой лайков - likes
* Число полученных сообщений - messages_received
* Число отправленных сообщений - messages_sent
* От скольких пользователей получили сообщения - users_received
* Скольким пользователям отправили сообщение - users_sent
* Срез - это os, gender и age
Нашу таблицу необходимо загрузить в схему test.

### Вот мой DAG: DAG_ETL_TVL

# Проект 2: Построение ETL-Пайплайна