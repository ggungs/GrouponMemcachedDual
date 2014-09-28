package kr.groupon.cache.memcache;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import kr.groupon.cache.exception.CacheException;
import kr.groupon.cache.exception.CacheNotFoundException;
import kr.groupon.cache.exception.CacheParseException;
import net.spy.memcached.MemcachedClientIF;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * 
 * Spymemcache를 이용한 캐시 2중화용 클래스
 * 
 * 로컬 클래스를 먼저 찾은 다음 없을 경우 중앙 캐시 서버를 사용한다.
 * 
 * @author ggungs
 * 
 */
@Component( "grouponMemcacheDual" )
public class GrouponMemcacheDual {

	@Autowired
	private RequestCacheObjectMap requestCacheObjectMap;

	private final Log log = LogFactory.getLog( getClass() );

	private final ObjectMapper mapper;
	/**
	 * 2nd Memcached Client
	 */
	private final MemcachedClientIF cacheDual;
	/**
	 * Central Memcached Client
	 */
	private final MemcachedClientIF cacheCentral;

	private final PlainTextTranscoder<Object> transcoder = new PlainTextTranscoder<Object>();

	/**
	 * 2nd Memcached Cache Expire Time
	 */
	public final static int CACHE_EXPIRE_TIME = 300;

	/**
	 * 처음 타켓을 어디로 할것인가?
	 */
	private final static int FIRST_TARGET_DUAL = 100;
	private final static int FIRST_TARGET_CENTRAL = 200;

	private final static int CACHE_FLAG_OBJECT = 1000;
	private final static int CACHE_FLAG_JSON = 2000;

	/**
	 * 생성자, 각 서버별로 캐시 클라이언트 클래스를 생성한다.
	 */
	@Autowired
	public GrouponMemcacheDual( @Qualifier( value = "memcachedCentral" ) MemcachedClientIF memcachedConnectionBean, @Qualifier( value = "memcachedDual" ) MemcachedClientIF memcachedConnectionBeanDual ) {

		cacheCentral = memcachedConnectionBean;
		cacheDual = memcachedConnectionBeanDual;

		mapper = new ObjectMapper();
		// NULL 객체를 넣자
		SerializationConfig sc = mapper.getSerializationConfig();
		sc.disable( SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS );
		mapper.setSerializationConfig( sc );

		// 해당하는 프로퍼티를 못찾아도 통과
		DeserializationConfig dc = mapper.getDeserializationConfig();
		mapper.setDeserializationConfig( dc );
	}

	/**
	 * 
	 * 이중화캐시가 존재하지 않아 중앙에서 가져와서 set을 할때 기본 expire time은 5분이다.
	 * 
	 * @param namespace 캐시 네임스페이스(cacheKey의 prefix)
	 * @param key 캐시 키, 네임스페이스와 :와 더하여 캐시키가 된다.
	 * @throws CacheException
	 * @throws CacheNotFoundException
	 * 
	 */
	public Object get( final String cacheKey ) throws CacheException, CacheNotFoundException {

		return get( cacheKey, CACHE_EXPIRE_TIME );
	}

	/**
	 * @param namespace 캐시 네임스페이스(cacheKey의 prefix)
	 * @param key 캐시 키, 네임스페이스와 :와 더하여 캐시키가 된다.
	 * @param expTime 캐시 익스파이어 타임
	 * 
	 */
	public Object get( final String cacheKey,
			int expTime ) throws CacheException, CacheNotFoundException {

		return getDualizeCache( CACHE_FLAG_OBJECT, cacheKey, expTime );
	}

	public <T> T getJSON( final String cacheKey,
			Class<T> clazz ) throws CacheException, CacheNotFoundException {

		return getJSON( cacheKey, CACHE_EXPIRE_TIME, clazz );
	}

	public <T> T getJSON( final String cacheKey,
			int expTime,
			Class<T> clazz ) throws CacheException, CacheNotFoundException {

		try {
			Object result = null;
			if ( requestCacheObjectMap.containsKey( cacheKey ) ) {
				logMapHit( cacheKey );
				result = requestCacheObjectMap.get( cacheKey );
			} else {
				result = getDualizeCache( CACHE_FLAG_JSON, cacheKey, CACHE_EXPIRE_TIME );
				if ( result == null ) {
					throw new CacheNotFoundException( cacheKey );
				}
			}

			if ( isValueNull( result ) ) {
				return null;
			}
			T t = mapper.readValue( ( String ) result, clazz );
			requestCacheObjectMap.put( cacheKey, ( String ) result );
			return t;
		} catch ( JsonParseException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( JsonMappingException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( IOException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		}
	}

	public <T> T getJSON( final String cacheKey,
			TypeReference<T> typeReference ) throws CacheException, CacheNotFoundException {

		return getJSON( cacheKey, CACHE_EXPIRE_TIME, typeReference );
	}

	public <T> T getJSON( final String cacheKey,
			int expTime,
			TypeReference<T> typeReference ) throws CacheException, CacheNotFoundException {

		try {
			Object result = null;
			if ( requestCacheObjectMap.containsKey( cacheKey ) ) {
				logMapHit( cacheKey );
				result = requestCacheObjectMap.get( cacheKey );
			} else {
				result = getDualizeCache( CACHE_FLAG_JSON, cacheKey, CACHE_EXPIRE_TIME );
				if ( result == null ) {
					throw new CacheNotFoundException( cacheKey );
				}
			}

			if ( isValueNull( result ) ) {
				return null;
			}
			T t = mapper.readValue( ( String ) result, typeReference );
			requestCacheObjectMap.put( cacheKey, ( String ) result );
			return t;
		} catch ( JsonParseException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( JsonMappingException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( IOException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		}
	}

	/**
	 * 
	 * 중앙 맴캐시에서 특정 네임스페이스와 키를 가진 캐시데이터를 가져옴
	 * 
	 * @param namespace
	 * @param key
	 * @return Cache Value
	 * @throws CacheException
	 * @throws CacheNotFoundException
	 */
	public Object getFromCentral( final String cacheKey ) throws CacheException, CacheNotFoundException {

		final Object result = cacheCentral.get( cacheKey );
		if ( result == null ) {
			logCacheNotFoundException( cacheKey );
			throw new CacheNotFoundException( cacheKey );
		}
		logCacheHit( cacheKey );
		return (result instanceof PertinentNegativeNull) ? null : result;
	}

	/**
	 * 
	 * 중앙 맴캐시에서 PLAINTEXT 특정 네임스페이스와 키를 가진 캐시데이터를 가져옴
	 * 
	 * @param namespace
	 * @param key
	 * @return Cache Value
	 * @throws CacheException
	 * @throws CacheNotFoundException
	 */
	public Object getFromCentralPlainText( final String cacheKey ) throws CacheException, CacheNotFoundException {

		final Object result = cacheCentral.get( cacheKey );
		if ( result == null ) {
			logCacheNotFoundException( cacheKey );
			throw new CacheNotFoundException( cacheKey );
		}
		logCacheHit( cacheKey );
		return result;
	}

	public <T> T getFromCentralJSON( final String cacheKey,
			Class<T> clazz ) throws CacheException, CacheNotFoundException {

		try {
			Object result = null;
			if ( requestCacheObjectMap.containsKey( cacheKey ) ) {
				logMapHit( cacheKey );
				result = requestCacheObjectMap.get( cacheKey );
			} else {
				result = getFromCentral( cacheKey );
				if ( result == null ) {
					throw new CacheNotFoundException( cacheKey );
				}
			}
			if ( isValueNull( result ) ) {
				return null;
			}

			T t = mapper.readValue( ( String ) result, clazz );
			requestCacheObjectMap.put( cacheKey, ( String ) result );
			return t;
		} catch ( JsonParseException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( JsonMappingException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( IOException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		}
	}

	public <T> T getFromCentralJSON( final String cacheKey,
			TypeReference<T> typeReference ) throws CacheException, CacheNotFoundException {

		try {
			Object result = null;
			if ( requestCacheObjectMap.containsKey( cacheKey ) ) {
				logMapHit( cacheKey );
				result = requestCacheObjectMap.get( cacheKey );
			} else {
				result = getFromCentral( cacheKey );
				if ( result == null ) {
					throw new CacheNotFoundException( cacheKey );
				}
			}
			if ( isValueNull( result ) ) {
				return null;

			}
			T t = mapper.readValue( ( String ) result, typeReference );
			requestCacheObjectMap.put( cacheKey, ( String ) result );
			return t;
		} catch ( JsonParseException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( JsonMappingException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		} catch ( IOException e ) {
			logCacheParseException( cacheKey, e );
			throw new CacheParseException( e );
		}
	}

	/**
	 * 
	 * 중앙 맴캐시에서 특정 네임스페이스와 키를 가진 캐시데이터를 저장
	 * 
	 * @param namespace
	 * @param key
	 * @return Cache Value
	 */
	public boolean setToCentral( final String cacheKey,
			int expTime,
			final Object obj ) throws CacheException {

		final Object submission = (obj == null) ? new PertinentNegativeNull() : obj;
		try {
			Future<Boolean> f = cacheCentral.set( cacheKey, expTime, submission );
			logCacheUpdate( cacheKey );
			return f.get();
		} catch ( InterruptedException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		} catch ( ExecutionException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		}
	}

	/**
	 * 
	 * 중앙 맴캐시에서 PLAINTEXT로 특정 네임스페이스와 키를 가진 캐시데이터를 저장
	 * 
	 * @param namespace
	 * @param key
	 * @return Cache Value
	 */
	public boolean setToCentralPlainText( final String cacheKey,
			int expTime,
			final Object obj ) throws CacheException {

		final Object submission = (obj == null) ? new PertinentNegativeNull() : obj;
		try {
			Future<Boolean> f = cacheCentral.set( cacheKey, expTime, submission, transcoder );
			logCacheUpdate( cacheKey );
			return f.get();
		} catch ( InterruptedException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		} catch ( ExecutionException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		}
	}

	public <T> boolean setToCentralJSON( final String cacheKey,
			int expTime,
			final T obj ) throws CacheException {

		Object submission = (obj == null) ? new PertinentNegativeNull() : obj;

		if ( submission != null ) {
			try {
				submission = mapper.writeValueAsString( submission );
			} catch ( JsonGenerationException e ) {
				logCacheParseException( cacheKey, e );
				throw new CacheParseException( e );
			} catch ( JsonMappingException e ) {
				logCacheParseException( cacheKey, e );
				throw new CacheParseException( e );
			} catch ( IOException e ) {
				logCacheParseException( cacheKey, e );
				throw new CacheParseException( e );
			}
		}

		try {
			Future<Boolean> f = cacheCentral.set( cacheKey, expTime, submission, transcoder );
			logCacheUpdate( cacheKey );
			return f.get();
		} catch ( InterruptedException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		} catch ( ExecutionException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		}
	}

	/**
	 * 
	 * 중앙 맴캐시에서 특정 네임스페이스와 키를 가진 캐시데이터를 삭제
	 * 
	 * @param namespace
	 * @param key
	 * @return Cache Value
	 */
	public boolean deleteToCentral( String cacheKey ) throws CacheException {

		Future<Boolean> f = null;
		try {
			f = cacheCentral.delete( cacheKey );
			return f.get();
		} catch ( InterruptedException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		} catch ( ExecutionException e ) {
			logCacheException( cacheKey, e );
			throw new CacheException( e );
		}
	}

	/**
	 * 중앙 멤캐시 서버의 데이터를 로컬에다가 복제(싱크)
	 * 
	 * @param cacheKey
	 * @param expTime
	 * @throws CacheException
	 */
	public void sync( String cacheKey,
			int expTime ) throws CacheException {

		final Object result = cacheCentral.get( cacheKey );
		final Object submission = (result instanceof PertinentNegativeNull) ? null : result;

		if ( submission != null ) {
			cacheDual.set( cacheKey, expTime, submission );
			log.debug( "[CACHE] SYNC CACHE : " + cacheKey );
		}
	}

	/**
	 * 중앙 멤캐시 서버의 데이터를 로컬에다가 복제(싱크)
	 * 
	 * @param cacheKey
	 * @param expTime
	 * @throws CacheException
	 */
	public void syncJSON( String cacheKey,
			int expTime ) throws CacheException {

		final Object result = cacheCentral.get( cacheKey );
		final Object submission = (result instanceof PertinentNegativeNull) ? null : result;

		if ( submission != null ) {
			cacheDual.set( cacheKey, expTime, submission, transcoder );
			log.debug( "[CACHE] SYNC CACHE : " + cacheKey );
		}
	}

	/**
	 * 캐시키를 생성한다.
	 * 
	 * @param namespace
	 * @param key
	 * @return CacheKey
	 */
	public String generateCachekey( String namespace,
			String key ) {

		return namespace + ":" + key;
	}

	/**
	 * 2중 캐시 서버가 동작하지 않을 경우 중앙 캐시서버에 접속하여 데이터를 가져오며
	 * 중앙 캐시 서버에도 값이 없는 경우 CacheNotFoundException를 throw 함
	 * 
	 * @param namespace 캐시 네임스페이스(cacheKey의 prefix)
	 * @param key 캐시 키, 네임스페이스와 :와 더하여 캐시키가 된다.
	 * @param expTime 이중화캐시가 존재하지 않아 중앙에서 가져와서 set을 할때 expire time
	 * @return 캐시 값
	 * 
	 * @throws CacheException 캐시를 사용하다가 익셉션 발생
	 * @throws CacheNotFoundException 캐시가 존재하지 않음
	 * 
	 */
	private Object getDualizeCache( int typeOfTranscode,
			String cacheKey,
			int expTime ) throws CacheException, CacheNotFoundException {

		// 처음 접속을 2중화 서버에 할 것인지 중앙서버에 할 것인지 여부
		int firstTarget = (cacheDual.getAvailableServers().size() == 0) ? FIRST_TARGET_CENTRAL : FIRST_TARGET_DUAL;

		if ( FIRST_TARGET_DUAL == firstTarget ) {
			final Object result = cacheDual.get( cacheKey );

			// 캐시 데이터가 있으면 리턴
			if ( result != null ) {
				log.debug( "[CACHE] DUAL CACHE HIT! : " + cacheKey );
				return (result instanceof PertinentNegativeNull) ? null : result;
			}
		}

		// 없으면 Central에서 get
		final Object result = getFromCentral( cacheKey );

		if ( result != null ) {
			log.debug( "[CACHE] CENTRAL CACHE HIT! : " + cacheKey );
			// Set Dual
			if ( typeOfTranscode == CACHE_FLAG_JSON ) {
				cacheDual.set( cacheKey, expTime, result, transcoder );
			} else {
				cacheDual.set( cacheKey, expTime, result );
			}

			log.debug( "[CACHE] SET DUAL CACHE HIT! : " + cacheKey );
			return (result instanceof PertinentNegativeNull) ? null : result;
		}

		logCacheNotFoundException( cacheKey );
		throw new CacheNotFoundException( cacheKey );
	}

	/**
	 * 
	 * @param result
	 * @return 상품이 NULL 인지 여부를 판단
	 */
	private boolean isValueNull( Object result ) {

		if ( result.toString().trim().equals( "[null]" ) ) {
			return true;
		}
		return (result instanceof PertinentNegativeNull) ? true : false;
	}

	private void logMapHit( String cacheKey ) {

		log.debug( "[CACHE] REQUEST MAP HIT! : " + cacheKey );
	}

	private void logCacheHit( String cacheKey ) {

		log.debug( "[CACHE] CACHE HIT! : " + cacheKey );
	}

	private void logCacheUpdate( String cacheKey ) {

		log.debug( "[CACHE] CACHE UPDATE! : " + cacheKey );
	}

	private void logCacheNotFoundException( String cacheKey ) {

		log.trace( "[CACHE] CACHE NOT FOUND : " + cacheKey );
	}

	private void logCacheException( String cacheKey,
			Throwable e ) {

		log.error( "[CACHE] CACHE EXCEPTION : " + cacheKey + ":" + e.getMessage() );
	}

	private void logCacheParseException( String cacheKey,
			Throwable e ) {

		log.error( "[CACHE] CACHE PARSING ERROR : " + cacheKey + ":" + e.getMessage() );
	}
}
