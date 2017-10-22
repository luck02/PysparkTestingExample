import Bootstrap
import sources


if __name__ == '__main__':
    session = Bootstrap.get_spark_session("test_app")

    ds1 = sources.get_derps(session)

